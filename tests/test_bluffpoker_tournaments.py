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
    assert not mtt["open"] and mtt["tournament_state"] == "registering" and mtt["start"] == NOW and mtt["seats"] == 8


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
        assert client.tables["table-2"]["tournament"] == {"format": "SNG", "players_left": 4, "players_total": 6, "places_paid": 2, "ante": None}
        await t.apply(SNG, {"status": "RUNNING", "myRegistrationStatus": "ELIMINATED", "myFinalPosition": 3, "info": info})
    asyncio.run(run())
    assert attached == ["table-1", "table-2"] and detached == ["table-1", "table-2"]
    assert [c.args[0] for c in client.join.await_args_list] == ["table-1", "table-2"]
    assert [c.args[0] for c in client.forget_table.await_args_list] == ["table-1", "table-2"]
    assert [e[2:] for e in events] == [("seated", {"table": "table-1"}), ("seated", {"table": "table-2"}), ("finished", {"place": 3})]
    assert events[0][1] == f"T{SNG}" and SNG not in t.active
