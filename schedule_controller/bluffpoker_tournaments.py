"""PC-worker side of BluffPoker SNG/MTT play, with the interface of tournament_worker.TournamentPlay.

BluffPoker announces nothing about tournaments on the table sockets: registration is a hub call, and the
player's tournament state (GET /player/tournaments/:id/state) says where the server seated the account
(myTableId), how the field stands (playersRemaining, info.entries, info.placesPaid) and when the account is out
(myRegistrationStatus ELIMINATED / WINNER, myFinalPosition). One follower per tournament polls that state.

Re-entry, rebuy and add-on (BluffPoker API of 2026-11-09): the state lists what the server accepts right now
(myReEntry, myRebuy, myAddOn, null = not offered). After an elimination the bot re-enters or takes a busted
rebuy, within the account's per-tournament limits and with its tournament rebuy chance; a seated rebuy (low
stack) and the add-on are decided once per offer. Registration has no idempotency key and is never retried
blindly; rebuy and add-on are retried only with the same key. Unregistering is not available (route not open).
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

from poker_controller.sites import table_key

from .tournament_worker import lobby_key

POLL_SECONDS = 5
OUT = {"ELIMINATED", "WINNER", "CANCELLED"}
OVER = {"FINISHED", "CANCELLED"}
# Registration is open while a tournament is SCHEDULED (an SNG stays so until its seats fill) or REGISTERING.
OPEN = {"SCHEDULED", "REGISTERING"}
# How long a finished tournament keeps being published as completed, so its lobby row stops showing it running.
COMPLETED_HOURS = 24
REGISTERED = {"REGISTERED", "CHECKED_IN", "PLAYING"}


def parse_time(value) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def lobby_rows(tournaments: list[dict], observed: datetime) -> list[dict]:
    """Lobby rows for the NLH SNGs and MTTs the scheduler may register for (registration open or upcoming), and
    the ones that ended in the last day as completed."""
    rows = []
    for t in tournaments or []:
        kind, status = t.get("type"), t.get("status")
        if kind not in ("SNG", "MTT") or t.get("gameType") != "NLH" or not t.get("id") or not t.get("name"):
            continue
        if status in OVER:
            ended = parse_time(t.get("actualEndAt")) or parse_time(t.get("scheduledStartAt"))
            if not ended or ended < observed - timedelta(hours=COMPLETED_HOURS):
                continue
        open_ = status in OPEN or (status == "RUNNING" and t.get("lateRegOpen") is True)
        state = "completed" if status in OVER else "running" if status == "RUNNING" else "registering"
        buy_in = Decimal(str(t.get("buyInTotalChips") or 0))
        registered = int(t.get("registeredCount") or t.get("entries") or 0)
        seats = int(t.get("sngMaxSeats") or t.get("maxEntries") or 0) or max(registered, int(t.get("tableSize") or 0), 1)
        rows.append({"id": lobby_key(t["id"]), "name": str(t["name"])[:200], "game": "NLH", "table_type": kind,
                     "bb": max(buy_in, Decimal("0.01")), "minimum": buy_in, "maximum": buy_in,
                     "seats": seats, "occupied": min(registered, seats), "open": open_,
                     "observed": observed, "snapshot": observed, "start": parse_time(t.get("scheduledStartAt")),
                     "tournament_state": state})
    return rows


class BluffPokerTournaments:
    """One account's BluffPoker tournaments: registration, seating, table moves, standings and the end."""

    def __init__(self, session, log):
        self.session = session
        self.log = log
        self.active: dict[str, dict] = {}   # tournament id -> state
        self.by_table: dict = {}            # play table key -> tournament id
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

    def emit(self, tid, event, **details):
        if self.on_event:
            self.on_event(self.policy["BotAccountId"], lobby_key(tid), event, details)

    def handle(self, message: dict):
        """EvenBet's tournament topics never arrive here."""

    def state_for(self, tid, fmt="SNG") -> dict:
        state = self.active.setdefault(str(tid), {"format": fmt, "entry_idx": 0, "table": None, "addon_decided": False})
        if not state.get("poller") or state["poller"].done():
            state["poller"] = self.spawn(self.follow(str(tid)), "standings")
        return state

    async def register(self, tid, fmt: str, entry_idx: int = 0, *, registration_only=False) -> dict:
        # No idempotency on BluffPoker's register: a failure is reported, never retried blindly.
        reply = await self.client.register_tournament(str(tid))
        if not registration_only:self.state_for(tid, fmt)["format"] = fmt
        self.log.info("REGISTERED %s %s=%s", self.policy["Username"], fmt, tid)
        return reply

    async def registered_tournaments(self) -> set:
        """The followed tournaments BluffPoker still holds a registration of this account for."""
        held = set()
        for tid in list(self.active):
            state = await self.client.tournament_state(tid)
            if state.get("myRegistrationStatus") in REGISTERED:
                held.add(tid)
        return held

    async def unregister(self, tid) -> None:
        raise ValueError("Unregistering is not available on BluffPoker yet")

    async def resume_registration(self,tid,fmt):
        tid=str(tid)
        reply=await self.client.tournament_state(tid)
        if reply.get('myRegistrationStatus') not in REGISTERED:return False
        self.state_for(tid,fmt)['format']=fmt
        await self.apply(tid,reply)
        return True

    def park_registration(self,tid):
        self.forget(tid)

    def forget(self, tid):
        state = self.active.pop(str(tid), None) or {}
        poller = state.get("poller")
        if poller and not poller.done() and poller is not asyncio.current_task():
            poller.cancel()

    async def resume(self):
        """Follow every tournament of the account again (after a login, reconnect or worker restart)."""
        async with self.resume_lock:
            for tid in list(self.active):
                state = self.active[tid]
                state["table"] = None  # the follower opens the current table again
                if not state.get("poller") or state["poller"].done():
                    state["poller"] = self.spawn(self.follow(tid), "standings")

    async def follow(self, tid: str):
        while tid in self.active:
            try:
                state = await self.client.tournament_state(tid)
                await self.apply(tid, state)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.warning("Tournament state failed for %s / %s: %s", self.policy["Username"], tid, exc)
            await asyncio.sleep(POLL_SECONDS)

    async def apply(self, tid: str, reply: dict):
        state = self.active.get(tid)
        if state is None:
            return
        info = reply.get("info") or {}
        state.update(players_left=reply.get("playersRemaining"), players_total=info.get("entries"),
                     places_paid=info.get("placesPaid"))
        mine = reply.get("myRegistrationStatus")
        if mine == "ELIMINATED" and reply.get("status") == "RUNNING" and await self.buy_back(tid, reply):
            return  # re-entered or rebought: the follower opens the table the server seats the account at
        if mine in OUT or reply.get("status") in OVER:
            await self.on_end(tid, reply.get("myFinalPosition"))
            return
        state.pop("bought_back_at", None)
        table = reply.get("myTableId")
        if table:
            await self.on_seated(tid, table_key(table))
        self.refresh_table_info(tid)
        if mine == "PLAYING":
            await self.offers_while_playing(tid, reply)

    # ---- re-entry, rebuy, add-on -----------------------------------------------------------------------------

    def limit(self, kind, fmt):
        """The account's maximum rebuys / re-entries per tournament (kind 'Rebuys' or 'Reentries'), per format."""
        value = self.policy.get(f"Max{'Mtt' if fmt == 'MTT' else 'Sng'}{kind}")
        return 3 if value is None and kind == "Rebuys" else 1 if value is None else int(value)

    def chance(self, column, default):
        value = self.policy.get(column)
        return default if value is None else float(value)

    async def pause(self, low, high):
        await asyncio.sleep(self.session.rng.uniform(low, high))

    async def affordable(self, offer) -> bool:
        try:
            summary = await self.client.balance()
        except Exception as exc:
            self.log.warning("Balance unknown for %s: %s", self.policy["Username"], exc)
            return True  # the hub refuses with INSUFFICIENT_CHIPS and charges nothing
        cost = int(offer.get("priceChips") or 0) + int(offer.get("feeChips") or 0)
        return int(summary.get("available") or 0) >= cost

    async def buy_back(self, tid: str, reply: dict) -> bool:
        """Eliminated in a running tournament: re-enter or take a busted rebuy, once per elimination. True while
        the account is (being) bought back in, so the tournament is not ended."""
        state = self.active[tid]
        if state.get("bought_back_at") is not None:
            # The hub seats the new entry asynchronously; the state may still read ELIMINATED for a moment.
            if asyncio.get_running_loop().time() - state["bought_back_at"] < 60:
                return True
            state.pop("bought_back_at", None)
        if state.get("elimination_decided"):
            return False
        state["elimination_decided"] = True
        table = state.get("table")
        if table:
            await self.session.detach_table(table)
            self.by_table.pop(table, None)
            await self.client.forget_table(table)
            state["table"] = None
        fmt = state.get("format")
        rebuy, reentry = reply.get("myRebuy"), reply.get("myReEntry")
        # Alternatives (rebuy price and rebuyMax, or re-entry price and reEntryMax): one of them, never both.
        if rebuy and state.get("rebuys", 0) < self.limit("Rebuys", fmt):
            kind, offer = "rebuy", rebuy
        elif reentry and state.get("reentries", 0) < self.limit("Reentries", fmt):
            kind, offer = "reentry", reentry
        else:
            return False
        if self.session.rng.random() >= self.chance("RebuyTournamentChance", .5):
            self.log.info("TOURNAMENT NO %s %s tournament=%s", kind.upper(), self.policy["Username"], tid)
            return False
        if not await self.affordable(offer):
            self.log.info("TOURNAMENT %s %s tournament=%s: balance too low", kind.upper(), self.policy["Username"], tid)
            return False
        await self.pause(3, 10)
        ok = await (self.rebuy(tid, busted=True) if kind == "rebuy" else self.reenter(tid))
        if ok:
            state["reentries" if kind == "reentry" else "rebuys"] = state.get("reentries" if kind == "reentry" else "rebuys", 0) + 1
            state["bought_back_at"] = asyncio.get_running_loop().time()
            state.pop("elimination_decided", None)
            self.emit(tid, kind, price=offer.get("priceChips"), fee=offer.get("feeChips"), chips=offer.get("chips"))
        return ok

    async def reenter(self, tid: str) -> bool:
        """Re-entry is a registration of an eliminated player: no idempotency key, so never retried blindly. After
        an unclear answer the state tells whether it went through (doc 2.6)."""
        try:
            reply = await self.client.register_tournament(tid)
            self.log.info("TOURNAMENT REENTRY %s tournament=%s debited=%s stack=%s", self.policy["Username"], tid,
                          reply.get("debitedChips"), reply.get("stackChips"))
            return True
        except Exception as exc:
            if getattr(exc, "terminal", False) and getattr(exc, "code", "") != "ALREADY_REGISTERED":
                self.log.info("TOURNAMENT REENTRY refused for %s / %s: %s", self.policy["Username"], tid, getattr(exc, "code", exc))
                return False
            state = await self.client.tournament_state(tid)
            done = state.get("myRegistrationStatus") == "PLAYING" or bool(state.get("myTableId")) or (state.get("myStack") or 0) > 0
            self.log.info("TOURNAMENT REENTRY %s tournament=%s unclear answer (%s); state says %s", self.policy["Username"],
                          tid, getattr(exc, "code", type(exc).__name__), "done" if done else "not done")
            return done

    async def purchase(self, tid: str, kind: str) -> bool:
        """A rebuy or add-on: one key per purchase, retried only with that key (doc 3.6)."""
        call = self.client.tournament_rebuy if kind == "rebuy" else self.client.tournament_addon
        key = f"{kind}-{uuid.uuid4()}"
        for attempt in range(6):
            try:
                reply = await call(tid, key)
            except Exception as exc:
                if getattr(exc, "terminal", False):
                    self.log.info("TOURNAMENT %s refused for %s / %s: %s", kind.upper(), self.policy["Username"], tid,
                                  getattr(exc, "code", exc))
                    return False
                await asyncio.sleep(2 + attempt * 2)  # timeout or 5xx: the same key, never a second purchase
                continue
            if reply.get("refunded"):
                self.log.info("TOURNAMENT %s refunded for %s / %s", kind.upper(), self.policy["Username"], tid)
                return False
            if reply.get("applied"):
                self.log.info("TOURNAMENT %s %s tournament=%s chips=%s debited=%s", kind.upper(), self.policy["Username"],
                              tid, reply.get("chips"), reply.get("debitedChips"))
                return True
            await asyncio.sleep(3)  # charged, not yet at the table: ask again with the same key
        self.log.warning("TOURNAMENT %s not confirmed for %s / %s after retries", kind.upper(), self.policy["Username"], tid)
        return False

    async def rebuy(self, tid: str, busted: bool = False) -> bool:
        return await self.purchase(tid, "rebuy")

    async def offers_while_playing(self, tid: str, reply: dict):
        """A seated rebuy (low stack) and the add-on, each decided once while the server offers it."""
        state = self.active[tid]
        fmt = state.get("format")
        offer = reply.get("myRebuy")
        if not offer:
            state.pop("rebuy_offer_decided", None)
        elif not state.get("rebuy_offer_decided"):
            state["rebuy_offer_decided"] = True
            if (state.get("rebuys", 0) < self.limit("Rebuys", fmt)
                    and self.session.rng.random() < self.chance("RebuyTournamentChance", .5)
                    and await self.affordable(offer)):
                await self.pause(2, 6)
                if await self.rebuy(tid):
                    state["rebuys"] = state.get("rebuys", 0) + 1
                    self.emit(tid, "rebuy", price=offer.get("priceChips"), fee=offer.get("feeChips"), chips=offer.get("chips"))
            else:
                self.log.info("TOURNAMENT NO REBUY %s tournament=%s", self.policy["Username"], tid)
        addon = reply.get("myAddOn")
        if addon and not state.get("addon_decided"):
            state["addon_decided"] = True
            if self.session.rng.random() < self.chance("AddonChance", .5) and await self.affordable(addon):
                await self.pause(2, 15)
                if await self.purchase(tid, "addon"):
                    self.emit(tid, "addon", price=addon.get("priceChips"), fee=addon.get("feeChips"), chips=addon.get("chips"))
            else:
                self.log.info("TOURNAMENT NO ADD-ON %s tournament=%s", self.policy["Username"], tid)

    async def on_seated(self, tid: str, table):
        state = self.active[tid]
        previous = state.get("table")
        if previous == table and table in self.session.bots:
            return
        if previous and previous != table:
            await self.session.detach_table(previous)
            self.by_table.pop(previous, None)
            await self.client.forget_table(previous)
            self.log.info("MOVED %s tournament=%s table %s -> %s", self.policy["Username"], tid, previous, table)
        state["table"] = table
        self.by_table[table] = tid
        self.client.tables.setdefault(table, {"i": table, "wire": str(table), "n": state.get("name") or f"Tournament {tid}"})
        self.refresh_table_info(tid)
        await self.client.join(table)  # the server already seated the account: no buy-in, no sit-down
        await self.session.attach_table(table)
        self.log.info("TOURNAMENT SEATED %s tournament=%s table=%s", self.policy["Username"], tid, table)
        self.emit(tid, "seated", table=table)

    async def on_end(self, tid: str, place):
        state = self.active.get(tid) or {}
        table = state.get("table")
        if table:
            await self.session.detach_table(table)
            self.by_table.pop(table, None)
            await self.client.forget_table(table)
        self.log.info("TOURNAMENT END %s tournament=%s place=%s", self.policy["Username"], tid, place)
        self.forget(tid)
        self.emit(tid, "finished", place=place)

    def refresh_table_info(self, tid):
        state = self.active.get(tid) or {}
        table = state.get("table")
        if table:
            self.client.tables.setdefault(table, {"i": table})["tournament"] = {
                "format": state.get("format"), "players_left": state.get("players_left"),
                "players_total": state.get("players_total"), "places_paid": state.get("places_paid"),
                "ante": None}

    def on_bust(self, table):
        """Busted at a tournament table: the hub marks the account ELIMINATED a moment later; the follower then
        decides on a re-entry or busted rebuy (doc 6: the state, not the socket, is the source of truth)."""
        self.log.info("TOURNAMENT BUST %s table=%s", self.policy["Username"], table)

    async def close(self):
        for task in list(self.tasks):
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
