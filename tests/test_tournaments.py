import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from schedule_controller.planner import Account, Entry, Machine
from schedule_controller.tournaments import Tournament, excluded_names, plan_registrations

NOW = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)
RANGES = {"low": (Decimal("0"), Decimal("2")), "medium": (Decimal("2.01"), Decimal("20")), "high": None}


def account(i, tier="low", **kw):
    return Account(i, 1, tier, **kw)


def machine(i=1, capacity=10):
    return Machine(i, 1, capacity, NOW)


def sng(i, registered, buy_in="2", max_players=6, **kw):
    return Tournament(f"T{i}", "SNG", Decimal(buy_in), registered, max_players, "registering", NOW, **kw)


def mtt(i, minutes, registered=5, buy_in="1"):
    return Tournament(f"T{i}", "MTT", Decimal(buy_in), registered, 3000, "registering", NOW, start=NOW + timedelta(minutes=minutes))


def run(fmt, target, accounts, tournaments, entries=(), next_time=None, machines=None):
    return plan_registrations(fmt, NOW, target, accounts, machines or [machine()], tournaments, list(entries), RANGES,
                              next_time, 20, 60, 60, 300, random.Random(1))


def test_sng_prefers_the_open_sng_with_the_fewest_free_seats_in_the_tier_range():
    plan = run("SNG", 2, [account(1)], [sng(1, 2), sng(2, 5), sng(3, 6), sng(4, 5, buy_in="50"), sng(5, 34, max_players=36)])
    # T3 is full and T4 is outside the low range (0-2); T5 has more players but 2 free seats, T2 only 1.
    assert [(a.account, a.tournament) for a in plan.actions] == [(1, "T2")]
    assert NOW + timedelta(seconds=20) <= plan.next_time <= NOW + timedelta(seconds=60)


def test_mtt_prefers_the_earliest_start_that_is_not_about_to_begin():
    plan = run("MTT", 1, [account(1)], [mtt(1, 1), mtt(2, 90), mtt(3, 30), mtt(4, 30, registered=9)])
    # T1 starts within two minutes; of the two at +30 min the fuller one wins.
    assert [a.tournament for a in plan.actions] == ["T4"]


def test_target_counts_only_that_formats_open_entries():
    tournaments = [sng(1, 3), mtt(2, 60)]
    entries = [Entry("e1", 2, 1, "T1"), Entry("e2", 3, 1, "T2"), Entry("e3", 4, 1, "ring-table")]
    assert run("SNG", 1, [account(1)], tournaments, entries).reasons == ["target_reached"]
    assert len(run("SNG", 2, [account(1)], tournaments, entries).actions) == 1


def test_registrations_are_paced_one_at_a_time():
    plan = run("SNG", 5, [account(1), account(2)], [sng(1, 3)], next_time=NOW + timedelta(seconds=10))
    assert plan.actions == [] and plan.reasons == ["paced"]
    plan = run("SNG", 5, [account(1), account(2)], [sng(1, 3)])
    assert len(plan.actions) == 1


def test_accounts_follow_the_ring_rules():
    tournaments = [sng(1, 3)]
    blocked = [account(1, enabled=False), account(2, credentials_ready=False), account(3, synthetic=True),
               account(4, session_end=NOW - timedelta(minutes=1)), account(5, eligible_after=NOW + timedelta(minutes=5)),
               account(6, max_tables=1), account(7, tier="high"), account(8, manual=True)]
    entries = [Entry("ring", 6, 1, "ring-table")]  # account 6 already plays its only table
    assert run("SNG", 5, blocked, tournaments, entries).reasons == ["no_eligible_account"]


def test_a_bot_does_not_register_twice_but_bots_may_share_a_tournament():
    entries = [Entry("e1", 1, 1, "T1")]
    plan = run("SNG", 5, [account(1), account(2)], [sng(1, 3)], entries)
    assert [a.account for a in plan.actions] == [2]


def test_machine_capacity_and_owner_are_respected():
    full = run("SNG", 5, [account(1)], [sng(1, 3)], [Entry("e", 9, 1, "x")], machines=[machine(1, capacity=1)])
    assert full.reasons == ["no_eligible_account"]
    pinned = run("SNG", 5, [account(1, owner_machine=2)], [sng(1, 3)], machines=[machine(1), machine(2)])
    assert [a.machine for a in pinned.actions] == [2]


def test_stale_completed_and_non_nlh_tournaments_are_ignored():
    old = Tournament("T1", "SNG", Decimal("1"), 3, 6, "registering", NOW - timedelta(hours=1))
    running = Tournament("T2", "SNG", Decimal("1"), 6, 6, "running", NOW)
    plo = Tournament("T3", "SNG", Decimal("1"), 3, 6, "registering", NOW, game="PLO4")
    assert run("SNG", 5, [account(1)], [old, running, plo]).reasons == ["no_open_tournament"]


def test_excluded_names_are_never_chosen_ignoring_case():
    shootout = Tournament("T1", "SNG", Decimal("2"), 34, 36, "registering", NOW, name="Shootout SNG")
    plain = Tournament("T2", "SNG", Decimal("2"), 1, 6, "registering", NOW, name="Double or Nothing")
    plan = plan_registrations("SNG", NOW, 5, [account(1)], [machine()], [shootout, plain], [], RANGES,
                              None, 20, 60, 60, 300, random.Random(1), excluded=["shootout"])
    assert [a.tournament for a in plan.actions] == ["T2"]
    only = plan_registrations("SNG", NOW, 5, [account(1)], [machine()], [shootout], [], RANGES,
                              None, 20, 60, 60, 300, random.Random(1), excluded=excluded_names("\n  SHOOTOUT \n\n"))
    assert only.reasons == ["no_open_tournament"]


def test_an_account_holds_at_most_its_sng_and_mtt_entry_limits():
    tournaments = [sng(1, 3), sng(2, 3), mtt(3, 60), mtt(4, 90)]
    # account 1 is registered for one SNG: with the default limit of 1 it gets no second SNG, but may take an MTT
    entries = [Entry("e1", 1, 1, "T1")]
    assert run("SNG", 5, [account(1)], tournaments, entries).reasons == ["no_eligible_account"]
    assert [a.account for a in run("MTT", 5, [account(1)], tournaments, entries).actions] == [1]
    # a higher limit allows a second SNG
    assert [a.tournament for a in run("SNG", 5, [account(1, max_sng_entries=2)], tournaments, entries).actions] == ["T2"]
    # a registration whose tournament left the lobby still counts
    assert run("SNG", 5, [account(1)], [sng(2, 3)], [Entry("e1", 1, 1, "Tgone")]).reasons == ["no_eligible_account"]
