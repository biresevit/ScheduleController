"""PC-worker side of SNG/MTT play: EvenBet tournament codes, lobby rows and per-account tournament state.

EvenBet sends many tournament fields as character codes:
    tt (type)   'G' (71) sit & go,  'S' (83) scheduled tournament (MTT)
    s  (state)  'G' (71) registering, 'R' (82) running, 'D' (68) done
    tp          'P' (80) spin & go (random prize, 3 players) -- not played
    g  (game)   72 NLH, 79 PLO;  p (limit) 'U' (85) no limit, 'P' (80) pot limit
    b           buy-in in minor units (cents); None for freerolls
    np / mx     registered players / maximum players;  sd start time (UTC), '0000-...' for SNGs
Tournament lobby rows use TableId 'T' + tournament id so they never collide with ring table ids.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

KINDS = {71: "SNG", 83: "MTT"}
STATES = {71: "registering", 82: "running", 68: "completed"}
SPIN = 80


def lobby_key(tournament_id) -> str:
    return f"T{tournament_id}"


def tournament_id(lobby_table_id: str) -> int | str:
    """EvenBet tournament ids are numbers; other platforms' (BluffPoker UUIDs) stay text."""
    text = str(lobby_table_id).removeprefix("T")
    return int(text) if text.isdigit() else text


def parse_start(value) -> datetime | None:
    if not value or str(value).startswith("0000"):
        return None
    return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def places_paid(distribution: list[dict]) -> int:
    """Paid places from a TournamentPrizeDistributionResponse table (rows carry 0-based pf/pt ranges)."""
    if not distribution:
        return 0
    ends = [int(r["pt"]) for r in distribution if r.get("pt") is not None]
    return max(ends) + 1 if ends else len(distribution)


# RebuyRequest types and the player command that declines a rebuy (as EvenBet's web client sends them).
REBUY, ADDON, DECLINE_REBUY = 82, 65, 7
TOPICS = ("TournamentInvite", "TournamentSeatedDown", "PlayerEndTournamentNotification",
          "PlayerInTournamentStatusNotification", "TournamentState", "TournamentClosed", "RebuyNotification")


class TournamentPlay:
    """One account's tournaments on its EvenBet connection: registration, start, table moves,
    standings for PDecision, rebuys / add-ons / re-entries, and the end.

    The owning Session supplies attach_table(tid) / detach_table(tid) to start and stop the table bot;
    on_event(account_id, lobby_key, event, details) tells the pilot to update the entry
    ('seated' with the play table, 'finished' with the place).
    """

    def __init__(self, session, log):
        self.session = session
        self.log = log
        self.active: dict[int, dict] = {}     # tournament id -> state
        self.by_table: dict[int, int] = {}    # play table id -> tournament id
        self.tasks: set = set()
        self.on_event = None
        self.resume_lock = asyncio.Lock()

    @property
    def client(self):
        return self.session.client

    @property
    def policy(self):
        return self.session.policy

    def spawn(self, coro, label):
        task = asyncio.create_task(self._guard(coro, label))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    async def _guard(self, coro, label):
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.log.warning("Tournament %s failed for %s: %s", label, self.policy["Username"], exc)

    async def pause(self, low, high):
        """A person's reaction time before a cashier decision."""
        await asyncio.sleep(self.session.rng.uniform(low, high))

    def chance(self, column, default):
        value = self.policy.get(column)
        return default if value is None else float(value)

    def emit(self, tid, event, **details):
        if self.on_event:
            self.on_event(self.policy["BotAccountId"], lobby_key(tid), event, details)

    # ---- registration ----------------------------------------------------------------------------

    async def register(self, tid: int, fmt: str, entry_idx: int = 0) -> dict:
        reply = await self.client.connection.request(
            "RegisterInTournament", acceptedExchangeRate=0, acceptedPrecision=0, grantTicketId=0, password="",
            paymentClubId=0, playerEntryIdx=entry_idx, srcCurrencyId=0, tournamentId=int(tid), useCurrencyConversion=False)
        if not reply.get("success"):
            raise ValueError(f"Tournament registration refused: {reply.get('text') or reply.get('resultCode') or reply}")
        state = self.active.setdefault(int(tid), {"format": fmt, "entry_idx": entry_idx, "table": None, "addon_decided": False})
        state.update(format=fmt, entry_idx=entry_idx, finished=False)
        if not state.get("poller"):
            state["poller"] = self.spawn(self.follow(int(tid)), "standings")
        self.log.info("REGISTERED %s %s=%s entry=%s", self.policy["Username"], fmt, tid, entry_idx)
        return reply

    async def registered_tournaments(self) -> set[int]:
        """Tournament ids EvenBet holds a registration of this account for."""
        reply = await self.client.connection.request("GetPlayerTournaments")
        return {int(t) for t in reply.get("tournaments") or []}

    async def unregister(self, tid: int) -> None:
        """Cancel a registration before the tournament starts (as the web client's Unregister button).

        Succeeds when EvenBet no longer lists the registration, also when it was already gone."""
        tid = int(tid)
        state = self.active.get(tid) or {}
        if state.get("table"):
            raise ValueError("The tournament has already seated this bot; it plays to the end")
        if tid in await self.registered_tournaments():
            await self.client.connection.request("UnregisterInTournament", tournamentId=tid,
                                                 playerEntryIdx=int(state.get("entry_idx") or 0))
            if tid in await self.registered_tournaments():
                raise ValueError("EvenBet kept the registration; its unregistration period may be over")
        self.forget(tid)
        self.log.info("UNREGISTERED %s tournament=%s", self.policy["Username"], tid)

    def forget(self, tid: int) -> None:
        """Stop following a tournament the account is no longer in."""
        state = self.active.pop(int(tid), None) or {}
        poller = state.get("poller")
        if poller and not poller.done():
            poller.cancel()

    # ---- messages --------------------------------------------------------------------------------

    def handle(self, message: dict):
        topic = message.get("t")
        if topic == "TournamentInvite":
            info = message.get("info") or {}
            self.spawn(self.on_invite(int(info.get("i") or message.get("tournamentId") or 0),
                                      int(message.get("playerEntryIdx") or 0)), "start")
        elif topic == "TournamentSeatedDown":
            info = message.get("info") or {}
            tid = int(message.get("tournamentId") or info.get("t") or 0)
            table = int(info.get("i") or 0)
            if tid and table:
                self.spawn(self.on_seated(tid, table, int(message.get("playerEntryIdx") or 0)), "seating")
        elif topic == "PlayerEndTournamentNotification":
            table = int(message.get("tableId") or 0)
            tid = int(message.get("tournamentId") or self.by_table.get(table) or 0)
            place = (message.get("playerInTournament") or {}).get("p")
            if tid:
                self.spawn(self.on_end(tid, table, place), "finish")
        elif topic == "PlayerInTournamentStatusNotification":
            tid = int(message.get("tournamentId") or 0)
            if tid in self.active:
                self.active[tid]["status"] = message
                self.maybe_addon(tid)
        elif topic == "TournamentState":
            tid = int(message.get("tournamentId") or 0)
            if tid in self.active and message.get("currentAnte") is not None:
                self.active[tid]["ante"] = message.get("currentAnte")
                self.refresh_table_info(tid)

    async def on_invite(self, tid, entry_idx):
        await self.client.connection.request("TournamentConfirm", tournamentId=tid, playerEntryIdx=entry_idx)
        await self.client.connection.request("SubscribeTournament", tournamentId=tid)
        self.active.setdefault(tid, {"format": "SNG", "entry_idx": entry_idx, "table": None, "addon_decided": False})
        self.log.info("TOURNAMENT START %s tournament=%s", self.policy["Username"], tid)

    async def on_seated(self, tid, table, entry_idx):
        state = self.active.setdefault(tid, {"format": "SNG", "entry_idx": entry_idx, "table": None, "addon_decided": False})
        previous = state.get("table")
        if previous == table and table in self.session.bots:
            # Login recovery may attach the bot before the pilot restores its
            # entry. Reconfirm the seat on the later recovery pass, without
            # selecting the table or starting another table worker.
            self.emit(tid, "seated", table=table)
            return
        if previous and previous != table:
            # Table balancing: the tournament moved the bot. Stop the old table bot without leaving.
            await self.session.detach_table(previous)
            self.by_table.pop(previous, None)
            self.log.info("MOVED %s tournament=%s table %s -> %s", self.policy["Username"], tid, previous, table)
        state.update(table=table, entry_idx=entry_idx)
        self.by_table[table] = tid
        self.client.tables.setdefault(table, {"i": table, "n": state.get("name") or f"Tournament {tid}", "g": 72})
        self.refresh_table_info(tid)
        # Every request for this table uses the tournament's entry index (the seat's TableState "ei" differs).
        self.client.tournament_entry_indices[table] = entry_idx
        if previous:
            self.client.tournament_entry_indices.pop(previous, None)
        # Open the table as EvenBet's web client does: select it, then claim the seat the tournament gave
        # us. Without the SitDown this connection only watches (hidden cards, auto-folded as away).
        await self.client.refresh_player_tables()
        await self.client.join(table)
        await self.client.take_tournament_seat(table)
        seat = self.client.hero_seat(self.client.states.get(table, {}))
        if seat is not None and self.client.states[table]["s"][seat].get("f", 0) & 1:
            await self.client.return_from_away(table)
        await self.session.attach_table(table)
        self.emit(tid, "seated", table=table)

    async def resume(self):
        """Re-attach tournament tables the account still sits at (after a login, reconnect or worker restart)."""
        async with self.resume_lock:
            await self._resume()

    async def _resume(self):
        for table in await self.client.refresh_player_tables():
            tid, table_id = table.get("t"), table.get("i")
            if not tid or not table_id:
                continue
            state = self.active.setdefault(int(tid), {"format": "SNG", "entry_idx": int(self.client.table_select_indices.get(int(table_id), 0)),
                                                      "table": None, "addon_decided": False})
            state["name"] = table.get("n")
            if not state.get("poller"):
                state["poller"] = self.spawn(self.follow(int(tid)), "standings")
            await self.client.connection.request("SubscribeTournament", tournamentId=int(tid))
            await self.on_seated(int(tid), int(table_id), int(state.get("entry_idx") or 0))

    async def on_end(self, tid, table, place):
        state = self.active.get(tid)
        if not state or state.get("finished"):
            return
        state["finished"] = True
        if table:
            await self.session.detach_table(table)
            self.by_table.pop(table, None)
        status = state.get("status") or {}
        # Re-entry, when the tournament still allows it, is decided with the tournament rebuy chance.
        if (status.get("isReEntryPeriodActive") and status.get("reEntryAllowedPersonal")
                and self.session.rng.random() < self.chance("RebuyTournamentChance", .5)):
            await self.pause(3, 10)
            try:
                await self.register(tid, state["format"], int(state.get("entry_idx") or 0) + 1)
                self.log.info("REENTRY %s tournament=%s", self.policy["Username"], tid)
                return
            except Exception as exc:
                self.log.warning("Re-entry failed for %s / %s: %s", self.policy["Username"], tid, exc)
        self.log.info("TOURNAMENT END %s tournament=%s place=%s", self.policy["Username"], tid, place)
        poller = state.get("poller")
        if poller and poller is not asyncio.current_task():
            poller.cancel()
        self.active.pop(tid, None)
        self.emit(tid, "finished", place=place)

    # ---- standings and purchases -----------------------------------------------------------------

    def refresh_table_info(self, tid):
        state = self.active.get(tid) or {}
        table = state.get("table")
        if table:
            self.client.tables.setdefault(table, {"i": table, "g": 72})["tournament"] = {
                "format": state.get("format"), "players_left": state.get("players_left"),
                "players_total": state.get("players_total"), "places_paid": state.get("places_paid"),
                "ante": state.get("ante")}

    async def follow(self, tid):
        """Standings for PDecision (players left / total, places paid) and rebuy/add-on chip amounts."""
        while tid in self.active and not self.active[tid].get("finished"):
            state = self.active[tid]
            try:
                info = (await self.client.connection.request("GetLobbyTournamentInfo", tournamentId=tid)).get("info") or {}
                state.update(name=info.get("n") or state.get("name"), players_total=info.get("np"),
                             players_left=info.get("na"), rebuy_chips=info.get("rcha"), addon_chips=info.get("acha"))
                if not state.get("places_paid") or STATES.get(info.get("s")) == "running":
                    prizes = await self.client.connection.request("TournamentPrizeDistributionRequest", tournamentId=tid)
                    state["places_paid"] = places_paid(prizes.get("distributionTable") or [])
                await self.client.connection.request("PlayerInTournamentStatusRequest", tournamentId=tid,
                                                     playerEntryIdx=int(state.get("entry_idx") or 0))
                self.refresh_table_info(tid)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.warning("Tournament standings failed for %s / %s: %s", self.policy["Username"], tid, exc)
            await asyncio.sleep(30)

    def on_bust(self, table):
        """The hero lost the whole stack at a tournament table: rebuy with the tournament rebuy chance
        while rebuys are open; otherwise wait for the end notification."""
        tid = self.by_table.get(table)
        if tid is not None:
            self.spawn(self.rebuy(tid), "rebuy")

    async def rebuy(self, tid):
        state = self.active.get(tid) or {}
        await self.pause(2, 6)
        status = state.get("status") or {}
        if not (status.get("isRebuyPeriodActive") and status.get("rebuyAllowedPersonal")):
            return
        entry_idx = int(state.get("entry_idx") or 0)
        if self.session.rng.random() < self.chance("RebuyTournamentChance", .5):
            reply = await self.client.connection.request("RebuyRequest", tournamentId=tid, type=REBUY,
                                                         playerEntryIdx=entry_idx, chipsAmount=int(state.get("rebuy_chips") or 0))
            ok = reply.get("success") or "planned" in str(reply.get("text", ""))
            self.log.info("TOURNAMENT REBUY %s tournament=%s ok=%s", self.policy["Username"], tid, bool(ok))
        else:
            await self.client.connection.request("TournamentPlayerCommand", command=DECLINE_REBUY, tournamentId=tid,
                                                 playerEntryIdx=entry_idx)
            self.log.info("TOURNAMENT NO REBUY %s tournament=%s", self.policy["Username"], tid)

    def maybe_addon(self, tid):
        state = self.active.get(tid) or {}
        status = state.get("status") or {}
        if state.get("addon_decided") or not (status.get("isAddonPeriodActive") and status.get("addonAllowedPersonal")):
            return
        state["addon_decided"] = True
        if self.session.rng.random() < self.chance("AddonChance", .5):
            self.spawn(self.addon(tid), "add-on")
        else:
            self.log.info("TOURNAMENT NO ADD-ON %s tournament=%s", self.policy["Username"], tid)

    async def addon(self, tid):
        state = self.active.get(tid) or {}
        await self.pause(2, 15)
        reply = await self.client.connection.request("RebuyRequest", tournamentId=tid, type=ADDON,
                                                     playerEntryIdx=int(state.get("entry_idx") or 0),
                                                     chipsAmount=int(state.get("addon_chips") or 0))
        self.log.info("TOURNAMENT ADD-ON %s tournament=%s ok=%s", self.policy["Username"], tid, bool(reply.get("success")))

    async def close(self):
        for task in list(self.tasks):
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)


def lobby_rows(tournaments: list[dict], observed: datetime) -> list[dict]:
    """Lobby rows for the NLH no-limit SNGs and MTTs the scheduler may register for."""
    rows = []
    for t in tournaments or []:
        kind, state = KINDS.get(t.get("tt")), STATES.get(t.get("s"))
        if (kind is None or state is None or t.get("g") != 72 or t.get("p") != 85 or t.get("tp") == SPIN
                or not t.get("n") or t.get("i") is None):
            continue
        buy_in = Decimal(t.get("b") or 0) / 100
        registered, maximum = int(t.get("np") or 0), int(t.get("mx") or 0)
        seats = max(maximum, registered, 1)
        rows.append({"id": lobby_key(t["i"]), "name": str(t["n"])[:200], "game": "NLH", "table_type": kind,
                     # A tournament row has no blinds; the buy-in keeps the BigBlind > 0 constraint meaningful.
                     "bb": max(buy_in, Decimal("0.01")), "minimum": buy_in, "maximum": buy_in,
                     "seats": seats, "occupied": min(registered, seats), "open": state == "registering",
                     "observed": observed, "snapshot": observed, "start": parse_start(t.get("sd")),
                     "tournament_state": state})
    return rows
