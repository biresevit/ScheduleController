"""PC-worker side of BluffPoker SNG/MTT play, with the interface of tournament_worker.TournamentPlay.

BluffPoker announces nothing about tournaments on the table sockets: registration is a hub call, and the
player's tournament state (GET /player/tournaments/:id/state) says where the server seated the account
(myTableId), how the field stands (playersRemaining, info.entries, info.placesPaid) and when the account is out
(myRegistrationStatus ELIMINATED / WINNER, myFinalPosition). One follower per tournament polls that state.

Not supported yet: rebuys, add-ons and re-entries (decided by BluffPoker's own clock: the bot plays on), and
unregistering (the hub route is not open).
"""
from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal

from poker_controller.sites import table_key

from .tournament_worker import lobby_key

POLL_SECONDS = 5
OUT = {"ELIMINATED", "WINNER", "CANCELLED"}
OVER = {"FINISHED", "CANCELLED"}
REGISTERED = {"REGISTERED", "CHECKED_IN", "PLAYING"}


def parse_time(value) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def lobby_rows(tournaments: list[dict], observed: datetime) -> list[dict]:
    """Lobby rows for the NLH SNGs and MTTs the scheduler may register for (registration open or upcoming)."""
    rows = []
    for t in tournaments or []:
        kind, status = t.get("type"), t.get("status")
        if kind not in ("SNG", "MTT") or t.get("gameType") != "NLH" or not t.get("id") or not t.get("name"):
            continue
        if status in OVER:
            continue
        open_ = status == "REGISTERING" or (status == "RUNNING" and t.get("lateRegOpen") is True)
        state = "running" if status == "RUNNING" else "registering"
        buy_in = Decimal(str(t.get("buyInTotalChips") or 0))
        registered = int(t.get("entries") or 0)
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

    async def register(self, tid, fmt: str, entry_idx: int = 0) -> dict:
        # No idempotency on BluffPoker's register: a failure is reported, never retried blindly.
        reply = await self.client.register_tournament(str(tid))
        self.state_for(tid, fmt)["format"] = fmt
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
        if reply.get("myRegistrationStatus") in OUT or reply.get("status") in OVER:
            await self.on_end(tid, reply.get("myFinalPosition"))
            return
        table = reply.get("myTableId")
        if table:
            await self.on_seated(tid, table_key(table))
        self.refresh_table_info(tid)

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
        """Busted at a tournament table: BluffPoker ends the tournament for the account (no rebuy yet)."""
        self.log.info("TOURNAMENT BUST %s table=%s", self.policy["Username"], table)

    async def close(self):
        for task in list(self.tasks):
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
