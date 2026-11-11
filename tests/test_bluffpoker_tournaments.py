import asyncio
import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

from schedule_controller.bluffpoker_tournaments import BluffPokerTournaments, lobby_rows

NOW = datetime(2026, 11, 5, 2, 0, tzinfo=timezone.utc)
SNG = "aaaaaaaa-0000-4000-8000-000000000001"


def row(**changes):
    r = {"id": SNG, "name": "SNG 6", "type": "SNG", "status": "REGISTERING", "gameType": "NLH", "buyInTotalChips": 22,
         "entries": 2, "sngMaxSeats": 6, "maxEntries": None, "tableSize": 6, "scheduledStartAt": None, "lateRegOpen": False}
    r.update(changes)
    return r


def test_lobby_rows_publish_open_and_upcoming_nlh_tournaments():
    rows = lobby_rows([row(), row(id="b", type="MTT", status="SCHEDULED", scheduledStartAt="2026-11-05T02:00:00.000Z", sngMaxSeats=None, maxEntries=None, entries=0, tableSize=8),
                       row(id="c", status="FINISHED"), row(id="d", gameType="PLO5")], NOW)
    assert [r["id"] for r in rows] == [f"T{SNG}", "Tb"]
    sng, mtt = rows
    assert sng["open"] and sng["table_type"] == "SNG" and sng["seats"] == 6 and sng["occupied"] == 2 and str(sng["bb"]) == "22"
    assert mtt["open"] and mtt["tournament_state"] == "registering" and mtt["start"] == NOW and mtt["seats"] == 8


def test_scheduled_sngs_take_registrations_and_finished_tournaments_publish_as_completed():
    # BluffPoker lists an SNG as SCHEDULED, without a start time, until its seats fill ("Iscrizioni aperte")
    rows = lobby_rows([row(status="SCHEDULED", entries=0, registeredCount=3),
                       row(id="recent", type="MTT", status="FINISHED", actualEndAt="2026-11-05T01:00:00.000Z"),
                       row(id="old", type="MTT", status="CANCELLED", scheduledStartAt="2026-11-03T01:00:00.000Z")], NOW)
    assert [(r["id"], r["open"], r["tournament_state"]) for r in rows] == [(f"T{SNG}", True, "registering"), ("Trecent", False, "completed")]
    assert rows[0]["occupied"] == 3


def test_running_mtt_is_open_only_during_explicit_late_registration():
    rows = lobby_rows([row(id="late", type="MTT", status="RUNNING", lateRegOpen=True),
                       row(id="closed", type="MTT", status="RUNNING", lateRegOpen=False),
                       row(id="unknown", type="MTT", status="RUNNING", lateRegOpen=None)], NOW)
    assert [(r["id"], r["open"], r["tournament_state"]) for r in rows] == [
        ("Tlate", True, "running"), ("Tclosed", False, "running"), ("Tunknown", False, "running")]


def test_follower_seats_moves_and_finishes_the_account():
    events, attached, detached = [], [], []
    client = SimpleNamespace(tables={}, join=AsyncMock(), forget_table=AsyncMock(), register_tournament=AsyncMock(return_value={"ok": True}))
    session = SimpleNamespace(client=client, bots={}, policy={"Username": "lato_b", "BotAccountId": 7})

    async def attach(table):
        attached.append(table); session.bots[table] = object()

    async def detach(table):
        detached.append(table); session.bots.pop(table, None)
    session.attach_table, session.detach_table = attach, detach
    t = BluffPokerTournaments(session, logging.getLogger("test"))
    t.on_event = lambda aid, key, event, details: events.append((aid, key, event, details))

    async def run():
        t.active[SNG] = {"format": "SNG", "table": None}
        info = {"entries": 6, "placesPaid": 2}
        await t.apply(SNG, {"status": "RUNNING", "myRegistrationStatus": "PLAYING", "myTableId": "table-1", "playersRemaining": 6, "info": info})
        await t.apply(SNG, {"status": "RUNNING", "myRegistrationStatus": "PLAYING", "myTableId": "table-1", "playersRemaining": 5, "info": info})
        await t.apply(SNG, {"status": "RUNNING", "myRegistrationStatus": "PLAYING", "myTableId": "table-2", "playersRemaining": 4, "info": info})
        assert client.tables["table-2"]["tournament"] == {"format": "SNG", "players_left": 4, "players_total": 6, "places_paid": 2, "ante": None, "buy_in": None}
        await t.apply(SNG, {"status": "RUNNING", "myRegistrationStatus": "ELIMINATED", "myFinalPosition": 3, "info": info})
    asyncio.run(run())
    assert attached == ["table-1", "table-2"] and detached == ["table-1", "table-2"]
    assert [c.args[0] for c in client.join.await_args_list] == ["table-1", "table-2"]
    assert [c.args[0] for c in client.forget_table.await_args_list] == ["table-1", "table-2"]
    assert [e[2:] for e in events] == [("seated", {"table": "table-1"}), ("seated", {"table": "table-2"}), ("finished", {"place": 3})]
    assert events[0][1] == f"T{SNG}" and SNG not in t.active


MTT = "bbbbbbbb-0000-4000-8000-000000000002"
OFFER = {"priceChips": 45, "feeChips": 5, "chips": 10000, "left": None}


class Uncertain(Exception):
    terminal, code = False, "TIMEOUT"


class Refused(Exception):
    terminal, code = True, "REGISTRATION_CLOSED"


def buyback_setup(monkeypatch, policy=None, **client_calls):
    async def no_sleep(_):
        return None
    monkeypatch.setattr("schedule_controller.bluffpoker_tournaments.asyncio.sleep", no_sleep)
    events, detached = [], []
    client = SimpleNamespace(tables={}, join=AsyncMock(), forget_table=AsyncMock(),
                             balance=AsyncMock(return_value={"available": 12000}), **client_calls)
    rng = SimpleNamespace(random=lambda: 0.0, uniform=lambda a, b: 0)
    session = SimpleNamespace(client=client, bots={}, rng=rng,
                              policy={"Username": "edo81", "BotAccountId": 9, "MaxMttReentries": 1, "MaxMttRebuys": 1,
                                      "RebuyTournamentChance": 1.0, "AddonChance": 1.0, **(policy or {})})

    async def attach(table):
        session.bots[table] = object()

    async def detach(table):
        detached.append(table); session.bots.pop(table, None)
    session.attach_table, session.detach_table = attach, detach
    t = BluffPokerTournaments(session, logging.getLogger("test"))
    t.on_event = lambda aid, key, event, details: events.append(event)
    t.active[MTT] = {"format": "MTT", "table": None}
    return t, client, events, detached


def eliminated(**changes):
    return {"status": "RUNNING", "myRegistrationStatus": "ELIMINATED", "myFinalPosition": 40, "myReEntry": OFFER, "myRebuy": None, **changes}


def test_eliminated_bot_reenters_within_its_limit_then_finishes(monkeypatch):
    t, client, events, detached = buyback_setup(monkeypatch, register_tournament=AsyncMock(return_value={"ok": True, "debitedChips": 50, "stackChips": 10000}))

    async def run():
        await t.apply(MTT, {"status": "RUNNING", "myRegistrationStatus": "PLAYING", "myTableId": "t1"})
        await t.apply(MTT, eliminated())                      # re-entry 1 of 1
        await t.apply(MTT, eliminated())                      # the hub has not seated the new entry yet: wait
        await t.apply(MTT, {"status": "RUNNING", "myRegistrationStatus": "PLAYING", "myTableId": "t2"})
        await t.apply(MTT, eliminated())                      # limit used: the tournament is over
    asyncio.run(run())
    assert client.register_tournament.await_count == 1
    assert events == ["seated", "reentry", "seated", "finished"]
    assert detached == ["t1", "t2"] and MTT not in t.active


def test_no_reentry_with_zero_chance_limit_or_balance(monkeypatch):
    for policy, balance in (({"RebuyTournamentChance": 0.0}, 12000), ({"MaxMttReentries": 0}, 12000), ({}, 10)):
        t, client, events, _ = buyback_setup(monkeypatch, policy=policy, register_tournament=AsyncMock())
        client.balance = AsyncMock(return_value={"available": balance})
        asyncio.run(t.apply(MTT, eliminated()))
        assert client.register_tournament.await_count == 0 and events == ["finished"], (policy, balance)


def test_unclear_reentry_answer_is_settled_by_the_state_never_resent(monkeypatch):
    t, client, events, _ = buyback_setup(monkeypatch, register_tournament=AsyncMock(side_effect=Uncertain()),
                                         tournament_state=AsyncMock(return_value={"myRegistrationStatus": "PLAYING", "myTableId": "t9"}))
    asyncio.run(t.apply(MTT, eliminated()))
    assert client.register_tournament.await_count == 1 and events == ["reentry"]
    t, client, events, _ = buyback_setup(monkeypatch, register_tournament=AsyncMock(side_effect=Refused()))
    asyncio.run(t.apply(MTT, eliminated()))
    assert events == ["finished"]


def test_busted_rebuy_is_preferred_and_retried_with_the_same_key(monkeypatch):
    rebuy = AsyncMock(side_effect=[Uncertain(), {"ok": True, "applied": False, "refunded": False},
                                   {"ok": True, "applied": True, "refunded": False, "chips": 10000, "debitedChips": 50}])
    t, client, events, _ = buyback_setup(monkeypatch, register_tournament=AsyncMock(), tournament_rebuy=rebuy)
    asyncio.run(t.apply(MTT, eliminated(myRebuy=OFFER)))
    keys = {c.args[1] for c in rebuy.await_args_list}
    assert rebuy.await_count == 3 and len(keys) == 1 and client.register_tournament.await_count == 0
    assert events == ["rebuy"]


def test_seated_rebuy_and_addon_are_decided_once_per_offer(monkeypatch):
    applied = {"ok": True, "applied": True, "refunded": False}
    t, client, events, _ = buyback_setup(monkeypatch, tournament_rebuy=AsyncMock(return_value=applied),
                                         tournament_addon=AsyncMock(return_value=applied))
    playing = {"status": "RUNNING", "myRegistrationStatus": "PLAYING", "myTableId": "t1", "myRebuy": OFFER, "myAddOn": OFFER}

    async def run():
        await t.apply(MTT, playing)
        await t.apply(MTT, playing)
    asyncio.run(run())
    assert client.tournament_rebuy.await_count == 1 and client.tournament_addon.await_count == 1
    assert events == ["seated", "rebuy", "addon"]


class AlreadyRegistered(Exception):
    terminal, code, status = False, "ALREADY_REGISTERED", 409


def test_already_registered_counts_as_a_successful_registration():
    for fmt, only in (("MTT", True), ("MTT", False), ("SNG", False)):
        client = SimpleNamespace(tables={}, register_tournament=AsyncMock(side_effect=AlreadyRegistered()),
                                 tournament_state=AsyncMock(return_value={"status": "REGISTERING", "myRegistrationStatus": "REGISTERED"}))
        session = SimpleNamespace(client=client, bots={}, policy={"Username": "someday87", "BotAccountId": 7})
        t = BluffPokerTournaments(session, logging.getLogger("test"))

        async def run():
            reply = await t.register("dc4aacf1", fmt, registration_only=only)
            followed = "dc4aacf1" in t.active
            for state in list(t.active.values()):
                state["poller"].cancel()
            return reply, followed
        reply, followed = asyncio.run(run())
        assert reply["success"] and reply["already_registered"], fmt
        assert followed is not only, (fmt, only)
        assert client.register_tournament.await_count == 1


def test_other_registration_refusals_still_fail():
    client = SimpleNamespace(tables={}, register_tournament=AsyncMock(side_effect=Refused()))
    session = SimpleNamespace(client=client, bots={}, policy={"Username": "someday87", "BotAccountId": 7})
    t = BluffPokerTournaments(session, logging.getLogger("test"))
    try:
        asyncio.run(t.register("dc4aacf1", "SNG"))
    except Refused:
        pass
    else:
        raise AssertionError("a closed registration must not count as registered")
    assert not t.active


def test_table_info_carries_the_lobby_buy_in_for_pdecision():
    from schedule_controller.tournament_worker import remember_buy_ins
    remember_buy_ins("BluffPokerTest", lobby_rows([row(buyInTotalChips=22)], NOW) + [{"id": "Tring", "table_type": "RING", "minimum": 5}])
    client = SimpleNamespace(tables={})
    session = SimpleNamespace(client=client, bots={}, platform="BluffPokerTest", policy={"Username": "someday87", "BotAccountId": 7})
    t = BluffPokerTournaments(session, logging.getLogger("test"))
    t.active[SNG] = {"format": "SNG", "table": "table-1"}
    t.refresh_table_info(SNG)
    assert client.tables["table-1"]["tournament"]["buy_in"] == 22
    session.platform = "OtherPlatform"
    t.refresh_table_info(SNG)
    assert client.tables["table-1"]["tournament"]["buy_in"] is None
