"""Satellite winners: the target MTT from the satellite's name, and adopting the seat BluffPoker registered."""
import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest

from schedule_controller.pilot import Pilot
from schedule_controller.satellites import is_satellite, satellite_target

NOW = datetime(2026, 11, 13, 20, 31, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def fixed_clock(monkeypatch):
    monkeypatch.setattr('schedule_controller.pilot.now',lambda:NOW)


def row(id, name, start, state="registering", kind="MTT"):
    return {"id": id, "name": name, "table_type": kind, "start": start, "tournament_state": state}


LOBBY = [row("Tsat", "Sat Basso Friday Main", NOW - timedelta(hours=1, minutes=31), "completed"),
         row("Talto", "Sat Alto Friday Main", NOW - timedelta(minutes=31), "running"),
         row("Tfm", "Friday Main", NOW - timedelta(minutes=1), "running"),
         row("Tfm2", "Friday Main 20/11", NOW + timedelta(days=7)),
         row("Tmain", "Main", NOW + timedelta(hours=1)),
         row("Tmc", "Monday Classic", NOW + timedelta(days=3)),
         row("Tsatmc", "Sat Alto -> Monday Classic", NOW + timedelta(hours=2))]


def test_target_is_the_longest_mtt_name_the_satellite_ends_with_earliest_start_first():
    assert is_satellite("Sat Basso Friday Main") and is_satellite("SAT Alto -> Monday Classic")
    assert not is_satellite("Saturday Big") and not is_satellite("Friday Main")
    assert satellite_target("Sat Basso Friday Main", LOBBY, NOW)["id"] == "Tfm"
    assert satellite_target("Sat Alto -> Monday Classic", LOBBY, NOW)["id"] == "Tmc"
    assert satellite_target("Sat Alto -> Unknown Event", LOBBY, NOW) is None
    assert satellite_target("Friday Main", LOBBY, NOW) is None


def pilot_with(status):
    p = Pilot.__new__(Pilot)
    p.tables = LOBBY
    p.policies = {7: {"Username": "dean"}}
    p.entries, p.sessions = {}, [{"id": "s1", "account": 7}]
    follow = []
    client = SimpleNamespace(tournament_state=AsyncMock(return_value={"myRegistrationStatus": status}))
    p.clients = {7: SimpleNamespace(client=client, tourneys=SimpleNamespace(state_for=lambda tid, fmt: follow.append((tid, fmt))))}
    p.publish = AsyncMock()
    p.park_tournament_registration = AsyncMock()
    return p, follow


def test_winner_registered_by_the_site_gets_an_entry_and_plays_the_running_target():
    p, follow = pilot_with("REGISTERED")
    satellite = {"id": "e-sat", "account": 7, "table": "Tsat", "session": "s1", "place": 2}
    e = asyncio.run(p.adopt_satellite_seat(7, satellite, checks=(0,)))
    assert e["table"] == "Tfm" and e["won_from"] == "e-sat" and e["status"] == "joining" and e["confirmed"]
    assert len(follow) == 1 and follow[0][1] == "MTT"
    assert not p.park_tournament_registration.await_count
    # once only: a second end event does not add another entry
    assert asyncio.run(p.adopt_satellite_seat(7, satellite, checks=(0,))) is None and len(p.entries) == 1


def test_no_registration_at_the_target_means_no_entry():
    p, follow = pilot_with(None)
    satellite = {"id": "e-sat", "account": 7, "table": "Tsat", "session": "s1", "place": 9}
    assert asyncio.run(p.adopt_satellite_seat(7, satellite, checks=(0,))) is None
    assert not p.entries and not follow


def test_a_future_target_is_parked_until_the_scheduler_wakes_it():
    p, follow = pilot_with("REGISTERED")
    satellite = {"id": "e-sat2", "account": 7, "table": "Tsatmc", "session": "s1", "place": 1}
    e = asyncio.run(p.adopt_satellite_seat(7, satellite, checks=(0,)))
    assert e["table"] == "Tmc" and p.park_tournament_registration.await_count == 1 and not follow
