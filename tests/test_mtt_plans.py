import random
import pytest
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


def run(now, plans, tournaments, accounts, progress=None, entries=(), next_time=None, rng=None, machines=None):
    for t in tournaments:  # observed just now
        object.__setattr__(t, "observed", now)
    return plan_mtt_registrations(now, plans, tournaments, progress or {}, accounts,
                                  machines if machines is not None else [Machine(1, 1, 50, now)],
                                  list(entries), RANGES, next_time, 20, 60, 60, 300, rng or random.Random(1))


def test_registers_by_the_plan_of_the_series_and_never_without_one():
    now = START - timedelta(hours=1)
    plans = {"Night Owl Freeroll": MttPlan(10, rebuy=10, addon=0)}
    plan = run(now, plans, [mtt(), mtt("Other MTT", i=2)], [account(1), account(2)])
    assert [(a.tournament, a.rebuy, a.addon) for a in plan.actions] == [("T1", True, False)] * 2
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
    assert [a.account for a in run(now, plans, [t], [account(1, "low"), account(2, "high")]).actions] == [2, 1]
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


@pytest.mark.parametrize('lead,allowed', [(60, True), (31, True), (30, False), (29, False)])
@pytest.mark.parametrize('ring_account,capacity', [(1, 1), (2, 1), (1, 2)])
def test_advance_registration_can_park_on_a_full_machine_and_full_account(lead, allowed, ring_account, capacity):
    now = START - timedelta(minutes=lead)
    plan = run(now, {'Night Owl Freeroll': MttPlan(10)}, [mtt()],
               [account(1, max_tables=1)], entries=[Entry('ring', ring_account, 1, 'ring-table')],
               machines=[Machine(1, 1, capacity, now)])
    assert bool(plan.actions) is allowed


def test_advance_registration_can_use_the_tier_fallback_on_a_full_machine():
    now = START - timedelta(hours=1)
    plan = run(now, {'Night Owl Freeroll': MttPlan(10)}, [mtt(buy_in='100')],
               [account(1, 'low')], entries=[Entry('ring', 2, 1, 'ring-table')],
               machines=[Machine(1, 1, 1, now)])
    assert [(a.account, a.machine) for a in plan.actions] == [(1, 1)]


@pytest.mark.parametrize('blocked', [
    account(1, manual=True), account(1, enabled=False), account(1, credentials_ready=False),
    account(1, requires_reconciliation=True), account(1, owner_machine=2),
    account(1, games=frozenset({'PLO4'})),
])
def test_parking_does_not_bypass_account_safety_or_manual_ownership(blocked):
    now = START - timedelta(hours=1)
    assert not run(now, {'Night Owl Freeroll': MttPlan(10)}, [mtt()], [blocked]).actions


def test_parked_registrations_still_obey_the_account_mtt_limit():
    now = START - timedelta(hours=1)
    entries = [Entry('waiting', 1, 1, 'T9', 'joining', parked=True)]
    assert not run(now, {'Night Owl Freeroll': MttPlan(10)}, [mtt()],
                   [account(1, max_mtt_entries=1)], entries=entries).actions


@pytest.mark.parametrize('machine', [
    Machine(1, 1, 1, START, ready=False), Machine(1, 1, 1, START, enabled=False),
    Machine(1, 1, 1, START, maintenance=True), Machine(1, 1, 0, START),
    Machine(1, 1, 1, START - timedelta(days=1)),
])
def test_parking_still_requires_an_available_machine(machine):
    now = START - timedelta(hours=1)
    assert not run(now, {'Night Owl Freeroll': MttPlan(10)}, [mtt()],
                   [account(1)], machines=[machine]).actions


def test_catchup_batch_is_bounded_staggered_and_does_not_mutate_inputs():
    now = START - timedelta(minutes=5)
    progress = {"T1": MttProgress()}
    accounts = [account(i) for i in range(20)]
    plan = run(now, {"Night Owl Freeroll": MttPlan(100)}, [mtt()], accounts, progress)
    assert len(plan.actions) == 5
    assert len({a.account for a in plan.actions}) == 5
    assert [int((a.not_before - now).total_seconds()) for a in plan.actions] == [0, 6, 12, 18, 24]
    assert progress["T1"].registered == 0 and not progress["T1"].accounts
    assert all(a.owner_machine is None for a in accounts)


def test_batch_counts_pending_places_and_preserves_rebuy_addon_quotas():
    now = START - timedelta(minutes=3)
    progress = {"T1": MttProgress(registered=7, rebuy=2, addon=1, accounts=set(range(7)))}
    plan = run(now, {"Night Owl Freeroll": MttPlan(10, rebuy=4, addon=2)}, [mtt()],
               [account(i) for i in range(20)], progress)
    # Pending reservations already contribute seven places; only three remain.
    assert len(plan.actions) == 3
    assert all(a.account not in set(range(7)) for a in plan.actions)
    assert sum(a.rebuy for a in plan.actions) == 2
    assert sum(a.addon for a in plan.actions) == 1


def test_deadline_pressure_beats_the_largest_raw_shortfall():
    now = START - timedelta(minutes=5)
    plans = {"Urgent": MttPlan(3), "Big": MttPlan(100)}
    ts = [mtt("Urgent", start=START - timedelta(minutes=2)), mtt("Big", start=START + timedelta(hours=1), i=2)]
    plan = run(now, plans, ts, [account(i) for i in range(20)])
    assert plan.actions[0].tournament == "T1"
    assert any(a.tournament == "T2" for a in plan.actions)


def test_batch_rechecks_live_machine_capacity():
    now = START - timedelta(minutes=5)
    plan = run(now, {"Night Owl Freeroll": MttPlan(100)}, [mtt()],
               [account(i) for i in range(20)], machines=[Machine(1, 1, 2, now)])
    assert len(plan.actions) == 2


def test_batch_pins_account_ownership_and_rechecks_format_limit():
    now = START - timedelta(hours=1)
    ts = [mtt("One"), mtt("Two", i=2), mtt("Three", i=3)]
    plan = run(now, {t.name: MttPlan(100) for t in ts}, ts,
               [account(1, max_mtt_entries=2)],
               machines=[Machine(1, 1, 50, now), Machine(2, 1, 50, now)])
    assert len(plan.actions) == 2
    assert len({a.tournament for a in plan.actions}) == 2
    assert len({a.machine for a in plan.actions}) == 1


def test_batch_never_queues_after_registration_closes():
    now = START - timedelta(minutes=2, seconds=10)
    plan = run(now, {"Night Owl Freeroll": MttPlan(100)}, [mtt()], [account(i) for i in range(20)])
    assert len(plan.actions) == 2
    assert all(a.not_before < START - timedelta(minutes=2) for a in plan.actions)


def test_batch_does_not_exceed_early_share_before_late_registration():
    now = START - timedelta(minutes=3)
    plan = run(now, {"Night Owl Freeroll": MttPlan(10, early_percent=30)}, [mtt(info=LATE)],
               [account(i) for i in range(20)])
    assert len(plan.actions) == 3

def test_overlapping_sunday_sized_plans_finish_with_registration_failures():
    from dataclasses import replace
    plans = {"Basso": MttPlan(120, early_percent=60),
             "Alto": MttPlan(60, early_percent=60),
             "Main": MttPlan(105, early_percent=90)}
    tournaments = [mtt(name, i=i, info={"late_reg": True})
                   for i, name in enumerate(plans, 1)]
    accounts = [account(i, max_mtt_entries=3) for i in range(500)]
    progress, entries, next_time = {}, [], None
    rng = random.Random(17)
    begin = START - timedelta(hours=2)
    attempts = 0
    for tick in range(351):  # two early hours, then 55 minutes (including the retry margin)
        now = begin + timedelta(seconds=tick * 30)
        current = [replace(t, observed=now, state='running' if now >= START else 'registering')
                   for t in tournaments]
        batch = plan_mtt_registrations(now, plans, current, progress, accounts,
                                      [Machine(1, 1, 1000, now)], entries, RANGES, next_time,
                                      45, 285, 60, 300, rng)
        next_time = batch.next_time
        for action in batch.actions:
            attempts += 1
            if attempts % 5 == 0:  # 20% of initial attempts fail and need replacement
                continue
            p = progress.setdefault(action.tournament, MttProgress())
            assert action.account not in p.accounts
            p.registered += 1
            p.accounts.add(action.account)
            entries.append(Entry(str(attempts), action.account, action.machine, action.tournament, 'joining'))
    assert {t.name: progress[t.id].registered for t in tournaments} == {name: p.total for name, p in plans.items()}
