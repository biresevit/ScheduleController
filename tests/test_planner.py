from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
import random

from schedule_controller.planner import Account, Entry, Machine, Settings, Table, plan
from schedule_controller.rules import Band, nenad_bands

NOW = datetime(2026, 10, 27, 9, tzinfo=timezone.utc)
BANDS = [Band("low", D(".02"), D(".5"), 10000), Band("medium", D(".5"), D(1), 0),
         Band("high", D(1), D(4), 0, True)]
SETTINGS = Settings(1, BANDS, min_gap=20, max_gap=20)


def run(accounts=None, machines=None, tables=None, entries=None, target=5, **kwargs):
    return plan(SETTINGS, NOW, target, accounts or [Account(1, 1, "low")],
                machines or [Machine(1, 1, 6, NOW)],
                tables or [Table("a", 1, D(".1"), 6, 0, NOW)], entries or [], random.Random(3), **kwargs)


def test_never_seats_account_twice_at_one_table():
    result = run()
    assert len(result.actions) == 1
    assert result.remaining_deficits["low"] == 4


def test_synthetic_accounts_cannot_generate_live_tasks():
    synthetic = [Account(1, 1, "low", synthetic=True, enabled=False, credentials_ready=False)]
    assert not run(accounts=synthetic).actions
    assert len(run(accounts=synthetic, simulation=True).actions) == 1


def test_owner_pins_account_to_one_machine_even_with_multiple_new_entries():
    result = run(machines=[Machine(1, 1, 6, NOW), Machine(2, 1, 6, NOW)],
                 tables=[Table(str(n), 1, D(".1"), 6, 0, NOW) for n in range(5)])
    assert len(result.actions) == 3
    assert len({a.machine for a in result.actions}) == 1


def test_first_tables_are_distributed_before_multitabling():
    accounts=[Account(n,1,'low') for n in range(1,4)]
    tables=[Table(str(n),1,D('.1'),6,0,NOW) for n in range(4)]
    result=run(accounts=accounts,tables=tables,target=3)
    assert len({a.account for a in result.actions}) == 3


def test_pending_reservations_consume_capacity_and_observed_seat_is_not_double_counted():
    a = Account(1, 1, "low")
    entries = [Entry("x", 1, 1, "a", "joining")]
    result = run(accounts=[a, Account(2, 1, "low")], machines=[Machine(1, 1, 1, NOW)],
                 tables=[Table("a", 1, D(".1"), 6, 1, NOW, observed_accounts=frozenset({1}))], entries=entries)
    assert not result.actions
    assert result.remaining_deficits["low"] == 4


def test_sitting_out_counts_toward_target():
    result = run(entries=[Entry("x", 1, 1, "a", "sitting_out")], target=1)
    assert not result.actions
    assert result.remaining_deficits["low"] == 0


def test_stale_machine_and_table_do_not_receive_joins():
    assert not run(machines=[Machine(1, 1, 6, NOW - timedelta(seconds=61))]).actions
    assert not run(tables=[Table("a", 1, D(".1"), 6, 0, NOW - timedelta(seconds=46))]).actions


def test_no_cross_site_or_cross_stake_or_currency_fallback():
    assert not run(accounts=[Account(1, 2, "low")]).actions
    assert not run(tables=[Table("a", 1, D(1), 6, 0, NOW)]).actions
    assert not run(tables=[Table("a", 1, D(".1"), 6, 0, NOW, currency="USD")]).actions


def test_empty_table_can_be_started_and_last_available_seat_can_be_filled():
    assert len(run(target=1).actions) == 1
    assert len(run(tables=[Table("a", 1, D(".1"), 6, 5, NOW)], target=1).actions) == 1


def test_under_daily_limit_is_preferred_then_least_played_over_limit():
    accounts = [Account(1, 1, "low", played_seconds=10000, max_minutes=120),
                Account(2, 1, "low", played_seconds=100, max_minutes=120)]
    result = run(accounts=accounts, target=1)
    assert result.actions[0].account == 2
    assert not result.actions[0].soft_limit_override
    accounts[1] = replace(accounts[1], played_seconds=9000)
    result = run(accounts=accounts, target=1)
    assert result.actions[0].account == 2
    assert result.actions[0].soft_limit_override


def test_spread_machine_risk_across_same_table():
    result = run(accounts=[Account(1, 1, "low", enabled=False), Account(2, 1, "low")],
                 machines=[Machine(1, 1, 6, NOW), Machine(2, 1, 6, NOW)],
                 entries=[Entry("x", 1, 1, "a")], target=2)
    assert result.actions[0].machine == 2


def test_table_concentration_takes_priority_over_total_machine_load():
    accounts = [Account(n, 1, "low", enabled=False) for n in range(1, 5)] + [Account(5, 1, "low")]
    entries = [Entry("1", 1, 1, "a")] + [Entry(str(n), n, 2, "b") for n in range(2, 5)]
    tables = [Table("a", 1, D(".1"), 6, 1, NOW, observed_accounts=frozenset({1})),
              Table("b", 1, D(".1"), 3, 3, NOW, observed_accounts=frozenset({2, 3, 4}))]
    result = run(accounts=accounts, entries=entries, tables=tables,
                 machines=[Machine(1, 1, 6, NOW), Machine(2, 1, 6, NOW)], target=5)
    assert result.actions[0].table == "a"
    assert result.actions[0].machine == 2


def test_high_frequency_is_more_likely_when_other_conditions_are_equal():
    accounts = [Account(1, 1, "low", frequency="low"), Account(2, 1, "low", frequency="high")]
    high_count = 0
    for seed in range(1000):
        result = plan(SETTINGS, NOW, 1, accounts, [Machine(1, 1, 6, NOW)],
                      [Table("a", 1, D(".1"), 6, 0, NOW)], [], random.Random(seed))
        high_count += result.actions[0].account == 2
    assert 730 < high_count < 870


def test_populated_table_is_preferred_without_being_mandatory():
    populated=0
    tables=[Table('empty',1,D('.1'),6,0,NOW),Table('busy',1,D('.1'),6,4,NOW)]
    for seed in range(1000):
        result=plan(SETTINGS,NOW,1,[Account(1,1,'low')],[Machine(1,1,6,NOW)],tables,[],random.Random(seed))
        populated+=result.actions[0].table=='busy'
    assert 930<populated<990


def test_cancel_reserved_joins_before_seated_departures_and_keep_departure_capacity():
    result = run(accounts=[Account(1, 1, "low"), Account(2, 1, "low")],
                 entries=[Entry("x", 1, 1, "a"), Entry("y", 2, 1, "a", "reserved")], target=0)
    assert [a.kind for a in result.actions] == ["cancel_join", "leave"]


def test_already_draining_entry_does_not_create_an_extra_departure():
    result = run(accounts=[Account(1, 1, "low"), Account(2, 1, "low")],
                 entries=[Entry("x", 1, 1, "a", "draining"), Entry("y", 2, 1, "a")], target=1)
    assert not result.actions


def test_unknown_observed_account_ownership_requires_reconciliation():
    result = run(tables=[Table("a", 1, D(".1"), 6, 1, NOW, observed_accounts=frozenset({1}))])
    assert not result.actions
    assert result.reasons == ["unreconciled_observed_entry"]


def test_duplicate_trigger_cannot_bypass_per_tick_budget():
    result = run(last_planned_at=NOW)
    assert not result.actions
    assert result.reasons == ["reconciliation_cooldown"]


def test_56_target_45_seated_creates_only_three_gradual_joins():
    accounts, entries, tables = [], [], []
    for index in range(9):
        tier = ("low", "medium", "high")[index // 3]
        ids = frozenset(range(index * 5 + 1, index * 5 + 6))
        for account_id in ids:
            accounts.append(Account(account_id, 1, tier, enabled=False))
            entries.append(Entry(str(account_id), account_id, account_id % 2 + 1, str(index)))
        tables.append(Table(str(index), 1, (D(".1"), D(".5"), D(2))[index // 3],
                            9, 5, NOW, observed_accounts=ids))
    for index in range(12):
        accounts.append(Account(100 + index, 1, ("low", "medium", "high")[index % 3]))
    result = plan(replace(SETTINGS, bands=nenad_bands()), NOW, 56, accounts,
                  [Machine(1, 1, 40, NOW), Machine(2, 1, 40, NOW)], tables, entries, random.Random(3))
    assert len(result.actions) == 3
    assert all(a.kind == "join" for a in result.actions)
    assert sum(result.remaining_deficits.values()) == 8
    assert [a.not_before for a in result.actions] == [NOW + timedelta(seconds=n) for n in (20, 40, 60)]


def test_target_reduction_creates_only_bounded_gradual_leaves():
    accounts=[Account(n,1,"low") for n in range(1,26)]
    tables=[Table(str(n),1,D(".1"),6,1,NOW,observed_accounts=frozenset({n})) for n in range(1,26)]
    entries=[Entry(str(n),n,1,str(n)) for n in range(1,26)]
    result=plan(SETTINGS,NOW,15,accounts,[Machine(1,1,30,NOW)],tables,entries,random.Random(3))
    assert len(result.actions)==3
    assert all(action.kind=="leave" for action in result.actions)
    assert [action.not_before for action in result.actions]==[NOW+timedelta(seconds=n) for n in (20,40,60)]


def test_expired_session_rotates_through_same_leave_pacing_budget():
    expired=Account(1,1,"low",session_end=NOW-timedelta(seconds=1))
    accounts=[expired,Account(2,1,"low")]
    tables=[Table(name,1,D(".1"),6,1,NOW,observed_accounts=frozenset({1})) for name in ("a","b","c")]
    entries=[Entry(name,1,1,name) for name in ("a","b","c")]
    settings=replace(SETTINGS,max_leaves=2)
    result=plan(settings,NOW,3,accounts,[Machine(1,1,6,NOW)],tables,entries,random.Random(3))
    assert [action.kind for action in result.actions]==["leave","leave"]
    assert [action.not_before for action in result.actions]==[NOW+timedelta(seconds=20),NOW+timedelta(seconds=40)]
    assert result.reasons==["expired_sessions_draining"]
