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


def test_scaled_slow_pace_queues_a_future_join_instead_of_discarding_every_gap():
    settings = replace(SETTINGS, min_gap=45, max_gap=285, tick_seconds=30,
                       planning_horizon_seconds=900, fill_minutes=45, session_minutes=640)
    inputs = ([Account(1, 1, 'low')], [Machine(1, 1, 6, NOW)],
              [Table('a', 1, D('.1'), 6, 0, NOW)], [])
    result = plan(settings, NOW, 27, *inputs, random.Random(3))
    assert len(result.actions) == 1
    assert NOW + timedelta(seconds=30) < result.actions[0].not_before < NOW + timedelta(seconds=900)
    assert result.next_join == result.actions[0].not_before
    # Keep the queued gap instead of drawing another until it is near due.
    repeated = plan(settings, NOW + timedelta(seconds=5), 27, *inputs, random.Random(9),
                    next_join=result.next_join)
    assert repeated.actions == []
    assert repeated.next_join == result.next_join


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


def test_under_daily_limit_is_preferred_then_every_over_limit_account_stays_eligible():
    accounts = [Account(1, 1, "low", played_seconds=10000, max_minutes=120),
                Account(2, 1, "low", played_seconds=100, max_minutes=120)]
    result = run(accounts=accounts, target=1)
    assert result.actions[0].account == 2
    assert not result.actions[0].soft_limit_override
    accounts[1] = replace(accounts[1], played_seconds=9000)
    chosen = set()
    for seed in range(40):
        result = plan(SETTINGS, NOW, 1, accounts, [Machine(1, 1, 6, NOW)],
                      [Table("a", 1, D(".1"), 6, 0, NOW)], [], random.Random(seed))
        assert result.actions[0].soft_limit_override
        chosen.add(result.actions[0].account)
    assert chosen == {1, 2}


def test_new_logins_come_before_extra_tables_even_past_the_daily_preference():
    # Only account 1 has time left; the others still get a first table before it gets a second.
    accounts = [Account(1, 1, "low", played_seconds=0, max_minutes=600)] + \
               [Account(n, 1, "low", played_seconds=50000, max_minutes=120) for n in (2, 3)]
    tables = [Table(str(n), 1, D(".1"), 6, 0, NOW) for n in range(6)]
    result = run(accounts=accounts, tables=tables, machines=[Machine(1, 1, 30, NOW)], target=3)
    assert [a.account for a in result.actions][0] == 1
    assert {a.account for a in result.actions} == {1, 2, 3}


def test_new_login_goes_to_the_least_loaded_pc():
    accounts = [Account(n, 1, "low", enabled=n == 9) for n in range(1, 10)]
    tables = [Table(str(n), 1, D(".1"), 9, 0, NOW) for n in range(9)]
    entries = [Entry(str(n), n, 1 if n <= 5 else 2, str(n)) for n in range(1, 8)]  # PC 1: 5/10, PC 2: 2/10
    for seed in range(20):
        result = plan(SETTINGS, NOW, 9, accounts, [Machine(1, 1, 10, NOW), Machine(2, 1, 10, NOW), Machine(3, 1, 20, NOW)],
                      tables, entries, random.Random(seed))
        assert result.actions[0].machine == 3  # 0/20 is the lightest


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
    assert 980<populated<1000


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


def test_future_action_is_not_followed_by_distant_queue_or_resampled_gap():
    settings=replace(SETTINGS,tick_seconds=30,planning_horizon_seconds=900,min_gap=45,max_gap=285)
    result=plan(settings,NOW,10,[Account(n,1,"low") for n in range(1,11)],
                [Machine(1,1,30,NOW)],[Table(str(n),1,D(".1"),6,0,NOW) for n in range(10)],[],random.Random(7))
    assert len(result.actions)==1
    # While the first action is still more than one tick away, no additional
    # commands are queued and its stored timing is retained.
    if result.next_join>NOW+timedelta(seconds=30):
        repeated=plan(settings,NOW,10,[Account(n,1,"low") for n in range(1,11)],
                      [Machine(1,1,30,NOW)],[Table(str(n),1,D(".1"),6,0,NOW) for n in range(10)],[],
                      random.Random(99),next_join=result.next_join)
        assert not repeated.actions and repeated.next_join==result.next_join


def test_update_maintenance_drains_only_selected_session_and_blocks_new_joins():
    accounts=[Account(1,1,'low',owner_machine=1,drain_for_update=True),Account(2,1,'low',owner_machine=1),Account(3,1,'low')]
    machines=[Machine(1,1,20,NOW,maintenance=True),Machine(2,1,20,NOW)]
    tables=[Table(str(i),1,D('.1'),6,1,NOW,observed_accounts=frozenset({1 if i<2 else 2})) for i in range(3)]
    entries=[Entry('a',1,1,'0'),Entry('b',1,1,'1'),Entry('c',2,1,'2')]
    result=plan(SETTINGS,NOW,4,accounts,machines,tables,entries,random.Random(5))
    leaves=[a for a in result.actions if a.kind=='leave']
    assert len(leaves)==2 and all(a.account==1 for a in leaves)
    assert all(a.machine==2 for a in result.actions if a.kind=='join')


def test_worker_sql_parameters_follow_configured_pc():
    from schedule_controller.pilot_repository import DB
    sql,values=DB(None,8,23).bind('INSERT example(SiteId,MachineId,Value) VALUES(:site,:machine,?)',('test',))
    assert sql=='INSERT example(SiteId,MachineId,Value) VALUES(?,?,?)'
    assert values==[8,23,'test']


def test_recovery_target_tracks_current_schedule_without_waiting_for_unneeded_entries():
    from schedule_controller.update_rollouts import recovery_target
    assert recovery_target(10,20,10)==8
    assert recovery_target(10,3,2)==1
    assert recovery_target(10,0,2)==0


def test_manual_session_gets_its_first_table_even_when_target_is_met_and_on_its_machine():
    tables = [Table(str(n), 1, D(".1"), 6, 0, NOW) for n in range(3)]
    machines = [Machine(1, 1, 6, NOW), Machine(2, 1, 6, NOW)]
    manual = Account(2, 1, "low", manual=True, owner_machine=2, session_end=NOW + timedelta(hours=1))
    result = run(accounts=[Account(1, 1, "low"), manual], machines=machines, tables=tables,
                 entries=[Entry("x", 1, 1, "0", "seated")], target=1)
    assert result.actions and all(a.account == 2 and a.machine == 2 for a in result.actions)
    assert result.remaining_deficits["low"] == 0


def test_manual_session_waits_for_start_but_ignores_cooldown():
    later = Account(1, 1, "low", manual=True, session_start=NOW + timedelta(minutes=5),
                    session_end=NOW + timedelta(hours=1))
    assert not run(accounts=[later], target=0).actions
    cooling = Account(1, 1, "low", manual=True, eligible_after=NOW + timedelta(minutes=30),
                      session_end=NOW + timedelta(hours=1))
    assert run(accounts=[cooling], target=0).actions


def test_manual_session_entries_are_not_trimmed_by_target():
    manual = Account(1, 1, "low", manual=True, session_end=NOW + timedelta(hours=1))
    result = run(accounts=[manual], entries=[Entry("x", 1, 1, "a", "seated")], target=0)
    assert not any(a.kind == "leave" for a in result.actions)


def test_stopped_manual_session_drains():
    stopped = Account(1, 1, "low", manual=True, session_end=NOW - timedelta(seconds=1))
    result = run(accounts=[stopped], entries=[Entry("x", 1, 1, "a", "seated")], target=0)
    assert [a.kind for a in result.actions] == ["leave"]


def test_manual_session_reports_why_it_cannot_join():
    offline = Machine(2, 1, 6, NOW - timedelta(minutes=5))
    manual = Account(1, 1, "low", manual=True, owner_machine=2, session_end=NOW + timedelta(hours=1))
    result = run(accounts=[manual], machines=[Machine(1, 1, 6, NOW), offline])
    assert not result.actions
    assert result.manual_blocked[1].startswith("machine not usable (2:")
    started_later = Account(1, 1, "low", manual=True, session_start=NOW + timedelta(minutes=5),
                            session_end=NOW + timedelta(hours=1))
    assert run(accounts=[started_later]).manual_blocked == {1: "session not started"}


def test_open_session_without_tables_is_refilled_before_a_new_session_starts():
    tables = [Table(str(n), 1, D(".1"), 6, 0, NOW) for n in range(2)]
    idle_session = Account(2, 1, "low", owner_machine=1, session_end=NOW + timedelta(hours=1))
    for seed in range(5):
        result = plan(SETTINGS, NOW, 5, [Account(1, 1, "low"), idle_session], [Machine(1, 1, 6, NOW)], tables, [],
                      random.Random(seed))
        assert result.actions[0].account == 2


def test_manual_session_seats_count_toward_the_target_and_scheduled_seats_make_room():
    tables = [Table(str(n), 1, D(".1"), 6, 0, NOW) for n in range(4)]
    manual = Account(2, 1, "low", manual=True, session_end=NOW + timedelta(hours=1))
    # the manual session has its table: no second one while the target (2) is met, and the scheduled seat over it leaves
    result = run(accounts=[Account(1, 1, "low"), manual], tables=tables, target=2,
                 entries=[Entry("x", 1, 1, "0", "seated"), Entry("y", 1, 1, "1", "seated"), Entry("z", 2, 1, "2", "seated")])
    assert [(a.kind, a.account) for a in result.actions] == [("leave", 1)]
    assert result.manual_blocked == {2: "ring target of 2 low entries reached"}
    # under the target, the manual session takes more tables
    result = run(accounts=[Account(1, 1, "low"), manual], tables=tables, target=3, entries=[Entry("z", 2, 1, "2", "seated")])
    assert result.actions and result.actions[0].account == 2


def test_paused_joins_still_let_ended_sessions_leave():
    # Joins paused on the Schedule page: the scheduler plans with max_joins=0.
    paused = replace(SETTINGS, max_joins=0)
    ended = Account(1, 1, "low", session_end=NOW - timedelta(minutes=1))
    result = plan(paused, NOW, 5, [ended, Account(2, 1, "low")], [Machine(1, 1, 6, NOW)],
                  [Table("a", 1, D(".1"), 6, 1, NOW, observed_accounts=frozenset({1})), Table("b", 1, D(".1"), 6, 0, NOW)],
                  [Entry("x", 1, 1, "a", "seated")], random.Random(3))
    assert [(a.kind, a.account) for a in result.actions] == [("leave", 1)]


def test_one_pc_fills_a_table_it_already_plays_before_opening_an_empty_one():
    # Simo: one PC, 20 bots spread one per table. A table with one of our bots now beats an empty one.
    accounts=[Account(1,1,'low',enabled=False),Account(2,1,'low')]
    tables=[Table('ours',1,D('.1'),6,1,NOW,observed_accounts=frozenset({1})),Table('empty',1,D('.1'),6,0,NOW)]
    joined=0
    for seed in range(1000):
        result=plan(SETTINGS,NOW,2,accounts,[Machine(1,1,20,NOW)],tables,[Entry('x',1,1,'ours')],random.Random(seed))
        joined+=result.actions[0].table=='ours'
    assert joined>900


def test_plo5_tables_are_joined_by_accounts_that_play_plo5_only():
    plo5=[Table('p5',1,D('.1'),6,0,NOW,game='PLO5')]
    assert run(accounts=[Account(1,1,'low')],tables=plo5).actions[0].table=='p5'
    assert not run(accounts=[Account(1,1,'low',games=frozenset({'NLH','PLO4'}))],tables=plo5).actions
    mixed=[Table('nlh',1,D('.1'),6,0,NOW),Table('p5',1,D('.1'),6,0,NOW,game='PLO5')]
    for seed in range(50):
        result=plan(SETTINGS,NOW,1,[Account(1,1,'low',games=frozenset({'PLO5'}))],[Machine(1,1,6,NOW)],mixed,[],random.Random(seed))
        assert result.actions[0].table=='p5'


def test_an_empty_format_still_gets_joins_next_to_busy_tables():
    # Every PLO5 table empty, NLH tables busy: the game is chosen first, so PLO5 still gets about half the joins.
    tables=[Table('busy',1,D('.1'),6,4,NOW),Table('p5',1,D('.1'),6,0,NOW,game='PLO5')]
    plo5=0
    for seed in range(1000):
        result=plan(SETTINGS,NOW,1,[Account(1,1,'low')],[Machine(1,1,6,NOW)],tables,[],random.Random(seed))
        plo5+=result.actions[0].table=='p5'
    assert 420<plo5<580



def test_extra_table_chance_adds_tables_to_playing_accounts():
    from dataclasses import replace as _replace
    import random as _random
    from schedule_controller import planner as _p
    now = datetime(2026, 11, 8, 20, 0, tzinfo=timezone.utc)
    bands = BANDS
    accounts = [_p.Account(i, 1, "low", max_tables=3) for i in range(1, 41)]
    machines = [_p.Machine(1, 1, 100, now)]
    tables = [_p.Table(f"t{i}", 1, D(".1"), 6, 2, now) for i in range(40)]
    entries = [_p.Entry(f"e{i}", i, 1, f"t{i}") for i in range(1, 11)]  # accounts 1-10 play one table each
    def joins(percent):
        settings = _p.Settings(1, bands, max_joins=1, extra_table_percent=percent, tick_seconds=30)
        extra = total = 0
        for seed in range(400):
            plan = _p.plan(settings, now, 100, accounts, machines, tables, entries, _random.Random(seed))
            made = [a for a in plan.actions if a.kind == "join"]
            total += len(made)
            extra += sum(1 for a in made if a.account <= 10)
        return extra / total
    assert joins(0) == 0                      # old behaviour: always a new login
    assert 0.2 <= joins(30) <= 0.42           # about 30% of the joins
    assert joins(100) == 1


def test_tournaments_only_account_never_joins_a_ring_table():
    only = Account(1, 1, "low", games=frozenset({"TOURNAMENTS"}))
    table = Table("t1", 1, D(".1"), 6, 2, NOW)
    plan_ = plan(Settings(1, BANDS, min_gap=20, max_gap=20, tick_seconds=30), NOW, 5, [only],
                 [Machine(1, 1, 10, NOW)], [table], [], random.Random(1))
    assert [a for a in plan_.actions if a.kind == "join"] == []


def _scaled(**kwargs):
    base = dict(min_gap=20, max_gap=60, tick_seconds=30, planning_horizon_seconds=210, fill_minutes=45,
                session_minutes=150, site_joins_per_minute=60)
    base.update(kwargs)
    return Settings(1, BANDS, **base)


def _crowd(n_accounts=300, n_tables=40, n_machines=1, capacity=10000):
    accounts = [Account(i, 1, "low", max_tables=4) for i in range(1, n_accounts + 1)]
    tables = [Table(f"t{i}", 1, D(".1"), 6, 0, NOW) for i in range(n_tables)]
    machines = [Machine(m, 1, capacity, NOW) for m in range(1, n_machines + 1)]
    return accounts, tables, machines


def test_scaled_pace_joins_faster_for_a_large_target_and_never_slower_than_fixed():
    accounts, tables, machines = _crowd(n_machines=10)
    big = plan(_scaled(), NOW, 1000, accounts, machines, tables, [], random.Random(1))
    joins = [a for a in big.actions if a.kind == "join"]
    # 1000 short: (400 + 1333) joins an hour -> ~2 s apart, so a 30 s pass plans far more than the old three.
    assert len(joins) > 6
    # One final join may carry a pause into a later tick; never queue a distant burst.
    assert sum(a.not_before > NOW + timedelta(seconds=30) for a in joins) <= 1
    assert all(a.not_before < NOW + timedelta(seconds=210) for a in joins)
    # One PC never logs in twice within min_gap.
    by_pc = {}
    for a in joins:
        by_pc.setdefault(a.machine, []).append(a.not_before)
    for stamps in by_pc.values():
        stamps.sort()
        assert all(b - a >= timedelta(seconds=20) for a, b in zip(stamps, stamps[1:]))
    # A small target keeps today's pace: no gap above the fixed maximum.
    small = plan(_scaled(), NOW, 5, accounts[:5], machines[:1], tables[:5], [], random.Random(1))
    assert small.actions and all(a.kind == 'join' for a in small.actions)
    assert sum(a.not_before > NOW + timedelta(seconds=30) for a in small.actions) <= 1
    assert small.actions[0].not_before - NOW <= timedelta(seconds=60)
    assert all(b.not_before - a.not_before <= timedelta(seconds=60)
               for a, b in zip(small.actions, small.actions[1:]))


def test_scaled_pace_respects_the_site_ceiling_and_pc_gap():
    accounts, tables, machines = _crowd(n_machines=30)
    result = plan(_scaled(site_joins_per_minute=6), NOW, 5000, accounts, machines, tables, [], random.Random(2))
    stamps = sorted(a.not_before for a in result.actions if a.kind == "join")
    assert all(b - a >= timedelta(seconds=10) for a, b in zip(stamps, stamps[1:]))
    # One PC with a login 5 s ago waits out its 20 s gap.
    one = [Machine(1, 1, 10000, NOW, last_join=NOW - timedelta(seconds=5))]
    waited = plan(_scaled(), NOW, 1000, accounts, one, tables, [], random.Random(2))
    first = min(a.not_before for a in waited.actions if a.kind == "join")
    assert first >= NOW + timedelta(seconds=15)


def test_scaled_pace_keeps_pauses_and_zero_fill_is_the_fixed_pace():
    accounts, tables, machines = _crowd()
    paused = plan(_scaled(max_joins=0), NOW, 1000, accounts, machines, tables, [], random.Random(3))
    assert not [a for a in paused.actions if a.kind == "join"]
    fixed = plan(_scaled(fill_minutes=0, max_joins=3), NOW, 1000, accounts, machines, tables, [], random.Random(3))
    assert len([a for a in fixed.actions if a.kind == "join"]) <= 3


def test_scaled_pace_rotates_many_ended_sessions_per_pass():
    accounts, tables, machines = _crowd(n_machines=10)
    ended = [replace(a, owner_machine=1 + a.id % 10, session_end=NOW - timedelta(minutes=1)) for a in accounts[:300]]
    entries = [Entry(f"e{a.id}", a.id, a.owner_machine, f"t{a.id % 40}") for a in ended]
    result = plan(_scaled(), NOW, 3000, ended + accounts[300:], machines, tables, entries, random.Random(4))
    assert len([a for a in result.actions if a.kind == "leave"]) > 3
