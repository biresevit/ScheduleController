import asyncio
import logging
import random
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from schedule_controller.tournament_worker import TournamentPlay, lobby_rows, places_paid

NOW = datetime(2026, 11, 1, 12, tzinfo=timezone.utc)


class Connection:
    def __init__(self, replies=None):
        self.sent = []
        self.replies = replies or {}

    async def request(self, topic, **fields):
        self.sent.append((topic, fields))
        reply = self.replies.get(topic, {})
        return reply(fields) if callable(reply) else reply


class Client:
    def __init__(self, connection):
        self.connection = connection
        self.tables, self.states, self.table_entry_indices, self.tournament_entry_indices = {}, {}, {}, {}
        self.table_select_indices = {}
        self.player_tables = []

    async def wait_state(self, table, predicate=None, timeout=20):
        self.states.setdefault(table, {"ti": table, "s": [{"i": 1, "f": 1}]})
        return self.states[table]

    async def refresh_player_tables(self):
        return self.player_tables

    async def join(self, table):
        await self.connection.request("SelectTable", tableId=table, playerEntryIdx=self.tournament_entry_indices.get(table, 0))
        await self.wait_state(table)

    async def take_tournament_seat(self, table):
        await self.connection.request("SitDown", tableId=table, cash=0)
        return 0

    def hero_seat(self, state):
        return 0 if state.get("s") else None

    async def return_from_away(self, table):
        await self.connection.request("ReturnFromAway", tableId=table)


class Session:
    def __init__(self, replies=None, policy=None, seed=1):
        self.client = Client(Connection(replies))
        self.policy = {"Username": "kate", "BotAccountId": 7, "RebuyTournamentChance": 1, "AddonChance": 1, **(policy or {})}
        self.rng = random.Random(seed)
        self.bots, self.attached, self.detached = {}, [], []

    async def attach_table(self, table):
        self.bots[table] = object(); self.attached.append(table)

    async def detach_table(self, table):
        self.bots.pop(table, None); self.detached.append(table)


def play(replies=None, **kw):
    session = Session(replies, **kw)
    t = TournamentPlay(session, logging.getLogger("test"))
    events = []
    t.on_event = lambda account, key, event, details: events.append((account, key, event, details))

    async def no_pause(low, high):
        await asyncio.sleep(0)
    t.pause = no_pause
    return session, t, events


async def settle(t):
    for _ in range(5):
        await asyncio.sleep(0)
    pending = [task for task in t.tasks if not task.done()]
    if pending:
        await asyncio.wait(pending, timeout=1)


def run(coro):
    return asyncio.run(coro)


def test_repeated_partial_lobby_scans_keep_sng_and_mtt_fresh():
    from unittest.mock import AsyncMock
    from types import SimpleNamespace
    from datetime import timedelta
    from poker_controller.client import EvenBetClient
    async def scan():
        client=EvenBetClient(connection=SimpleNamespace(listeners=[],request=AsyncMock(side_effect=[
            {'tournaments':[{'i':42,'n':'Sandstorm','tt':71,'g':72,'p':85,'s':71,'b':100,'np':2,'mx':6},
                            {'i':43,'n':'Daily MTT','tt':83,'g':72,'p':85,'s':71,'b':200,'np':5,'mx':100}]},
            {'tournaments':[{'i':42},{'i':43,'np':6}]},
            {'tournaments':[{'i':42,'s':68},{'i':43}]}])))
        first=lobby_rows(await client.list_tournaments(),NOW)
        later=lobby_rows(await client.list_tournaments(),NOW+timedelta(minutes=2))
        assert [r['id'] for r in first]==[r['id'] for r in later]==['T42','T43']
        assert all(r['observed']==NOW+timedelta(minutes=2) for r in later)
        assert later[0]['name']=='Sandstorm' and later[1]['occupied']==6
        finished=lobby_rows(await client.list_tournaments(),NOW+timedelta(minutes=3))
        assert finished[0]['tournament_state']=='completed'
        assert finished[1]['tournament_state']=='registering'
    run(scan())


def test_registration_success_and_refusal():
    async def go():
        ok = {"RegisterInTournament": {"t": "RegisterInTournamentsResponse", "success": True}}
        session, t, _ = play(ok)
        await t.register(123, "SNG")
        topic, fields = session.client.connection.sent[0]
        assert topic == "RegisterInTournament" and fields["tournamentId"] == 123 and fields["playerEntryIdx"] == 0
        assert t.active[123]["format"] == "SNG"
        await t.close()
        session, t, _ = play({"RegisterInTournament": {"success": False, "text": "Not enough money"}})
        with pytest.raises(ValueError, match="Not enough money"):
            await t.register(5, "MTT")
    run(go())


def test_start_seat_move_and_finish():
    async def go():
        session, t, events = play()
        t.handle({"t": "TournamentInvite", "info": {"i": 9}, "playerEntryIdx": 0})
        await settle(t)
        assert [x[0] for x in session.client.connection.sent] == ["TournamentConfirm", "SubscribeTournament"]
        t.handle({"t": "TournamentSeatedDown", "tournamentId": 9, "info": {"i": 501, "t": 9}, "playerEntryIdx": 0})
        await settle(t)
        assert session.attached == [501] and t.by_table == {501: 9}
        sent = [topic for topic, _ in session.client.connection.sent]
        assert sent[-3:] == ["SelectTable", "SitDown", "ReturnFromAway"]
        assert session.client.tournament_entry_indices == {501: 0}
        assert session.client.tables[501]["tournament"]["format"] == "SNG"
        # Table balancing: moved to 502 without leaving 501.
        t.handle({"t": "TournamentSeatedDown", "tournamentId": 9, "info": {"i": 502, "t": 9}, "playerEntryIdx": 0})
        await settle(t)
        assert session.detached == [501] and session.attached == [501, 502] and t.by_table == {502: 9}
        assert session.client.tournament_entry_indices == {502: 0}
        assert not any(topic == "LeaveTable" for topic, _ in session.client.connection.sent)
        t.handle({"t": "PlayerEndTournamentNotification", "tableId": 502, "playerInTournament": {"p": 3}})
        await settle(t)
        assert session.detached == [501, 502] and 9 not in t.active
        assert [e[2] for e in events] == ["seated", "seated", "finished"]
        assert events[-1] == (7, "T9", "finished", {"place": 3})
    run(go())


def test_reentry_uses_the_tournament_rebuy_chance():
    async def go():
        ok = {"RegisterInTournament": {"success": True}}
        session, t, events = play(ok, policy={"RebuyTournamentChance": 1})
        t.active[9] = {"format": "MTT", "entry_idx": 0, "table": 501, "addon_decided": False,
                       "status": {"isReEntryPeriodActive": True, "reEntryAllowedPersonal": True}}
        t.by_table[501] = 9
        await t.on_end(9, 501, 120)
        assert session.client.connection.sent[0][0] == "RegisterInTournament"
        assert session.client.connection.sent[0][1]["playerEntryIdx"] == 1
        assert [e[2] for e in events] == []  # still in the tournament
        await t.close()
    run(go())


def test_rebuy_and_addon_follow_their_chances():
    async def go():
        replies = {"RebuyRequest": {"success": True}}
        session, t, _ = play(replies, policy={"RebuyTournamentChance": 1, "AddonChance": 0})
        t.active[9] = {"format": "MTT", "entry_idx": 0, "table": 501, "rebuy_chips": 1000, "addon_chips": 1500, "addon_decided": False,
                       "status": {"isRebuyPeriodActive": True, "rebuyAllowedPersonal": True,
                                  "isAddonPeriodActive": True, "addonAllowedPersonal": True}}
        t.by_table[501] = 9
        await t.rebuy(9)
        assert session.client.connection.sent[-1] == ("RebuyRequest", {"tournamentId": 9, "type": 82, "playerEntryIdx": 0, "chipsAmount": 1000})
        t.maybe_addon(9)
        await settle(t)
        assert t.active[9]["addon_decided"] and not any(f.get("type") == 65 for _, f in session.client.connection.sent)

        session, t, _ = play(replies, policy={"RebuyTournamentChance": 0, "AddonChance": 1})
        t.active[9] = {"format": "MTT", "entry_idx": 2, "table": 501, "rebuy_chips": 1000, "addon_chips": 1500, "addon_decided": False,
                       "status": {"isRebuyPeriodActive": True, "rebuyAllowedPersonal": True,
                                  "isAddonPeriodActive": True, "addonAllowedPersonal": True}}
        await t.rebuy(9)
        assert session.client.connection.sent[-1] == ("TournamentPlayerCommand", {"command": 7, "tournamentId": 9, "playerEntryIdx": 2})
        t.maybe_addon(9)
        await settle(t)
        assert ("RebuyRequest", {"tournamentId": 9, "type": 65, "playerEntryIdx": 2, "chipsAmount": 1500}) in session.client.connection.sent
    run(go())


def test_places_paid_from_the_prize_table():
    # As EvenBet sent it for a 6-player SNG: place 0 (no range) and places 1..1.
    assert places_paid([{"mpa": 390, "mpp": 0.65}, {"pf": 1, "pt": 1, "mpa": 210, "mpp": 0.35}]) == 2
    assert places_paid([{"pf": 0, "pt": 0}, {"pf": 1, "pt": 2}, {"pf": 3, "pt": 8}]) == 9
    assert places_paid([]) == 0


def test_lobby_rows_keep_only_nlh_sngs_and_mtts():
    listing = [
        {"i": 1, "n": "SNG Sandstorm", "tt": 71, "s": 71, "g": 72, "p": 85, "tp": 84, "b": 100, "np": 2, "mx": 6, "sd": "0000-00-00 00:00:00"},
        {"i": 2, "n": "Sunday Special", "tt": 83, "s": 71, "g": 72, "p": 85, "tp": 84, "b": 50000, "np": 18, "mx": 3000, "sd": "2026-11-01 20:00:00"},
        {"i": 3, "n": "Spin 1 PM", "tt": 71, "s": 71, "g": 72, "p": 85, "tp": 80, "b": 92, "mx": 3},
        {"i": 4, "n": "PLO", "tt": 83, "s": 71, "g": 79, "p": 80, "b": 5000, "mx": 3000},
        {"i": 5, "n": "Freeroll", "tt": 83, "s": 82, "g": 72, "p": 85, "b": None, "np": 40, "mx": 3000, "sd": "2026-11-01 08:00:00"},
    ]
    rows = {r["id"]: r for r in lobby_rows(listing, NOW)}
    assert sorted(rows) == ["T1", "T2", "T5"]
    assert rows["T1"]["table_type"] == "SNG" and rows["T1"]["minimum"] == Decimal("1") and rows["T1"]["start"] is None
    assert rows["T2"]["table_type"] == "MTT" and rows["T2"]["start"] == datetime(2026, 11, 1, 20, tzinfo=timezone.utc)
    assert rows["T5"]["tournament_state"] == "running" and rows["T5"]["bb"] == Decimal("0.01") and not rows["T5"]["open"]


def test_resume_reattaches_running_tournament_tables():
    async def go():
        session, t, events = play()
        session.client.player_tables = [{"i": 777, "t": 42, "n": "Double or Nothing"}, {"i": 5, "n": "ring table"}]
        await t.resume()
        assert session.attached == [777] and t.by_table == {777: 42}
        assert ("SubscribeTournament", {"tournamentId": 42}) in session.client.connection.sent
        assert events[-1][1:3] == ("T42", "seated")
        await t.close()
    run(go())


def test_unregister_cancels_a_registration_and_confirms_it_is_gone():
    async def go():
        registered = {9, 10}
        replies = {"GetPlayerTournaments": lambda f: {"tournaments": sorted(registered)},
                   "UnregisterInTournament": lambda f: registered.discard(f["tournamentId"]) or {"t": "Result", "resultCodeExt": -1}}
        session, t, _ = play(replies)
        t.active[9] = {"format": "SNG", "entry_idx": 0, "table": None, "addon_decided": False}
        await t.unregister(9)
        assert ("UnregisterInTournament", {"tournamentId": 9, "playerEntryIdx": 0}) in session.client.connection.sent
        assert 9 not in t.active and registered == {10}
        # Already gone on EvenBet (e.g. unregistered by hand): nothing to send, still a success.
        sent = len(session.client.connection.sent)
        await t.unregister(9)
        assert [x[0] for x in session.client.connection.sent[sent:]] == ["GetPlayerTournaments"]
    run(go())


def test_unregister_refuses_once_seated_or_when_evenbet_keeps_the_registration():
    async def go():
        session, t, _ = play({"GetPlayerTournaments": {"tournaments": [9]}})
        t.active[9] = {"format": "SNG", "entry_idx": 0, "table": 501, "addon_decided": False}
        with pytest.raises(ValueError, match="already seated"):
            await t.unregister(9)
        t.active[9]["table"] = None
        with pytest.raises(ValueError, match="kept the registration"):
            await t.unregister(9)
        assert 9 in t.active
    run(go())


def test_concurrent_resumes_attach_a_tournament_table_once():
    async def go():
        session, t, events = play()
        session.client.player_tables = [{"i": 777, "t": 42, "n": "Sandstorm"}]
        await asyncio.gather(t.resume(), t.resume())
        assert session.attached == [777] and t.by_table == {777: 42}
        await t.close()
    run(go())


@pytest.mark.parametrize('prior_status',['joining','seated','uncertain'])
def test_restart_seating_before_entry_restoration_is_reconfirmed(prior_status):
    """Reproduce Monica/Mason: login attaches before pilot entry adoption."""
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from schedule_controller.pilot import Pilot

    async def go():
        session,t,_=play()
        session.tourneys=t
        session.client.player_tables=[{'i':777,'t':42,'n':'Sandstorm'}]
        session.client.list_tables=AsyncMock()
        p=Pilot.__new__(Pilot)
        p.entries={};p.sessions=[];p.policies={7:session.policy};p.clients={7:session}
        p.close_session_if_idle=AsyncMock(return_value=False)
        adopted=[];published=[]
        p.repo=SimpleNamespace(adopt_reconciled_entry=lambda *args:adopted.append(args))
        async def publish():
            published.append([(e['status'],e.get('play_table')) for e in p.entries.values()])
        p.publish=publish
        t.on_event=p.tournament_event
        async def session_for(aid):
            await t.resume() # First seating event arrives with no restored entry.
            assert not p.entries
            return session
        p.session_for=session_for
        row=dict(BotAccountId=7,EntryId='entry',TableId='T42',Status=prior_status,
                 BuyInAmount=None,CreatedUtc=NOW,SeatedUtc=None,SessionId=None,Username='kate')
        await p.reconcile_cloud_entries([row])
        await asyncio.sleep(0)
        e=p.entries['entry']
        assert e['status']=='seated' and e['play_table']=='777' and e['confirmed']
        assert published[-1]==[('seated','777')]
        # Reconfirmation must not resend a seat claim, create another bot, or
        # detach the table (which would end existing live/manual views).
        assert session.attached==[777] and not session.detached
        assert [topic for topic,_ in session.client.connection.sent].count('SitDown')==1
        assert len(adopted)==1
        await t.close()
    run(go())
