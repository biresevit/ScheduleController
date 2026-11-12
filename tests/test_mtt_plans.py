import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from schedule_controller.mtt_plans import (MttPlan, desired, open_for, recurrence, registration_window, series_name,
                                           split, take)
from schedule_controller.planner import Account, Entry, Machine
from schedule_controller.tournaments import MttProgress, Tournament, plan_mtt_registrations

START = datetime(2026, 11, 12, 22, 0, tzinfo=timezone.utc)
RANGES = {"low": (Decimal("0"), Decimal("2")), "high": (Decimal("50"), Decimal("500"))}
LATE = {"late_reg": True, "late_reg_until": "2026-11-12T23:40:00.000Z"}


def test_series_name_drops_the_date_so_one_plan_covers_every_recurrence():
    assert series_name("Night Owl Freeroll 12/11") == "Night Owl Freeroll"
    assert series_name("TEST NLH 2min 15k 05/11 03:00") == "TEST NLH 2min 15k"
    assert series_name("Super Tuesday Ƀ20K GTD") == "Super Tuesday Ƀ20K GTD"
    assert series_name("11 Satellite to Sunday Special") == "11 Satellite to Sunday Special"
    assert series_name("Freeroll 12.11.2026") == "Freeroll"


def test_default_split_is_40_percent_early_unless_there_is_no_late_registration():
    plan = MttPlan(10)
    assert split(plan, late=True) == (4, 6)
    assert split(plan, late=False) == (10, 0)
    assert split(MttPlan(5, early_percent=100), late=True) == (5, 0)


def test_early_registrations_ramp_over_the_hours_before_the_start():
    plan = MttPlan(10, early_hours=2)
    assert desired(plan, START, START - timedelta(hours=3), LATE) == 0
    assert desired(plan, START, START - timedelta(hours=2), LATE) == 0
    assert desired(plan, START, START - timedelta(hours=1), LATE) == 3       # half of 4, rounded up
    assert desired(plan, START, START - timedelta(minutes=2), LATE) == 4
    # Late registration: 6 more over 80% of the 100 minutes until it closes.
    assert desired(plan, START, START + timedelta(minutes=40), LATE) == 4 + 3
    assert desired(plan, START, START + timedelta(minutes=80), LATE) == 10
    # Without late registration everyone registers before the start.
    assert desired(plan, START, START - timedelta(minutes=2), {}) == 10


def test_late_registration_window_defaults_to_an_hour_when_its_end_is_unknown():
    first, last = registration_window(MttPlan(10), START, {"late_reg": True})
    assert first == START - timedelta(hours=2) and last == START + timedelta(minutes=48)


def test_open_for_registering_before_the_start_and_late_only_when_the_lobby_allows_it():
    plan = MttPlan(10)
    assert open_for(plan, START, START - timedelta(hours=1), "registering", True, {})
    assert not open_for(plan, START, START + timedelta(minutes=5), "running", True, {})
    assert open_for(plan, START, START + timedelta(minutes=5), "running", True, LATE)
    assert not open_for(plan, START, START + timedelta(minutes=5), "running", False, LATE)
    assert not open_for(plan, START, START + timedelta(hours=2), "running", True, LATE)


def test_rebuy_and_addon_places_are_exact_once_every_bot_registered():
    rng = random.Random(3)
    for rebuy in range(0, 11):
        got = 0
        for remaining in range(10, 0, -1):
            got += take(rebuy - got, remaining, rng.random())
        assert got == rebuy


def test_recurrence_pattern():
    day = [START + timedelta(days=i) for i in range(4)]
    assert recurrence(day) == {"recurring": True, "count": 4, "pattern": "daily"}
    assert recurrence([START]) == {"recurring": False, "count": 1, "pattern": None}
    assert recurrence([START, START + timedelta(days=7)])["pattern"] == "weekly"


def account(i, tier="low", **kw):
    return Account(i, 1, tier, **kw)


def mtt(name="Night Owl Freeroll 12/11", start=START, state="registering", buy_in="0", info=None, i=1, is_open=True):
    return Tournament(f"T{i}", "MTT", Decimal(buy_in), 0, 3000, state, START - timedelta(hours=3), start=start,
                      name=name, open=is_open, info=info or {})


def run(now, plans, tournaments, accounts, progress=None, entries=(), next_time=None, rng=None):
    for t in tournaments:  # observed just now
        object.__setattr__(t, "observed", now)
    return plan_mtt_registrations(now, plans, tournaments, progress or {}, accounts, [Machine(1, 1, 50, now)],
                                  list(entries), RANGES, next_time, 20, 60, 60, 300, rng or random.Random(1))


def test_registers_by_the_plan_of_the_series_and_never_without_one():
    now = START - timedelta(hours=1)
    plans = {"Night Owl Freeroll": MttPlan(10, rebuy=10, addon=0)}
    plan = run(now, plans, [mtt(), mtt("Other MTT", i=2)], [account(1), account(2)])
    assert [(a.tournament, a.rebuy, a.addon) for a in plan.actions] == [("T1", True, False)]
    assert run(now, {}, [mtt()], [account(1)]).reasons == ["on_plan"]


def test_on_plan_registrations_wait_for_the_gap_but_a_late_plan_catches_up_at_once():
    now = START - timedelta(hours=1)
    plans = {"Night Owl Freeroll": MttPlan(10)}  # no late registration: all 10 over 118 minutes, 6 wanted by now
    gap = now + timedelta(seconds=30)
    assert run(now, plans, [mtt()], [account(1)], {"T1": MttProgress(registered=6)}, next_time=gap).reasons == ["on_plan"]
    assert run(now, plans, [mtt()], [account(1)], {"T1": MttProgress(registered=5)}, next_time=gap).reasons == ["paced"]
    assert run(now, plans, [mtt()], [account(1)], {"T1": MttProgress(registered=5)}).actions
    assert run(now, plans, [mtt()], [account(1)], {"T1": MttProgress(registered=3)}, next_time=gap).actions


def test_an_account_registers_once_per_instance_even_after_busting():
    now = START + timedelta(minutes=10)
    plans = {"Night Owl Freeroll": MttPlan(10, early_percent=0)}
    progress = {"T1": MttProgress(registered=1, accounts={1})}
    plan = run(now, plans, [mtt(state="running", info=LATE)], [account(1)], progress)
    assert not plan.actions and plan.reasons == ["no_eligible_account:T1"]
    plan = run(now, plans, [mtt(state="running", info=LATE)], [account(1), account(2)], progress)
    assert [a.account for a in plan.actions] == [2]


def test_tier_buy_in_range_is_preferred_but_not_required():
    now = START - timedelta(minutes=5)
    plans = {"Main": MttPlan(2)}
    t = mtt("Main", buy_in="100")
    assert [a.account for a in run(now, plans, [t], [account(1, "low"), account(2, "high")]).actions] == [2]
    assert [a.account for a in run(now, plans, [t], [account(1, "low")]).actions] == [1]


def test_late_registration_only_while_the_lobby_shows_it_open():
    now = START + timedelta(minutes=10)
    plans = {"Night Owl Freeroll": MttPlan(10)}
    assert run(now, plans, [mtt(state="running", info=LATE)], [account(1)]).actions
    assert not run(now, plans, [mtt(state="running", info=LATE, is_open=False)], [account(1)]).actions
    assert not run(now, plans, [mtt(state="running")], [account(1)]).actions


def test_account_mtt_limit_and_open_entries_still_apply():
    now = START - timedelta(minutes=5)
    plans = {"Night Owl Freeroll": MttPlan(10)}
    busy = [Entry("e1", 1, 1, "T9", "joining")]
    t9 = Tournament("T9", "MTT", Decimal("0"), 0, 3000, "registering", now, start=START + timedelta(days=1), name="X")
    plan = run(now, plans, [mtt(), t9], [account(1, max_mtt_entries=1)], entries=busy)
    assert not plan.actions
