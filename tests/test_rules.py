from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal as D
import random

import pytest

from schedule_controller.rules import (Band, Window, choose_buy_in, generate_defaults, nenad_bands,
                                       split_interval, stake_targets, target_at, validate_bands, validate_windows)


def utc(value):
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def test_tuesday_schedule_boundaries_and_uncovered_hours():
    windows = [Window(1, 600, 660, 56)]
    assert target_at(windows, utc("2026-10-27T08:59:59"), "Europe/Budapest") == 0
    assert target_at(windows, utc("2026-10-27T09:00:00"), "Europe/Budapest") == 56
    assert target_at(windows, utc("2026-10-27T10:00:00"), "Europe/Budapest") == 0


def test_both_fall_back_hours_follow_local_schedule():
    windows = [Window(6, 120, 180, 8)]
    assert target_at(windows, utc("2026-10-25T00:30:00"), "Europe/Budapest") == 8
    assert target_at(windows, utc("2026-10-25T01:30:00"), "Europe/Budapest") == 8


def test_missing_spring_forward_hour_never_activates():
    windows = [Window(6, 120, 180, 8)]
    assert target_at(windows, utc("2026-03-29T00:59:00"), "Europe/Budapest") == 0
    assert target_at(windows, utc("2026-03-29T01:00:00"), "Europe/Budapest") == 0


def test_reject_overlaps_and_naive_clock():
    with pytest.raises(ValueError, match="Overlapping"):
        validate_windows([Window(1, 600, 700, 1), Window(1, 650, 720, 2)])
    with pytest.raises(ValueError, match="timezone"):
        target_at([], datetime(2026, 1, 1), "Europe/Budapest")


def test_account_time_uses_utc_duration_and_local_day_boundary():
    parts = split_interval(utc("2026-10-24T21:30:00"), utc("2026-10-25T23:00:00"), "Europe/Budapest")
    assert parts == {"2026-10-24": 1800, "2026-10-25": 25 * 3600}
    spring = split_interval(utc("2026-03-28T23:00:00"), utc("2026-03-29T22:00:00"), "Europe/Budapest")
    assert spring == {"2026-03-29": 23 * 3600}


@pytest.mark.parametrize("value,tier", [("0.019", None), ("0.02", "low"), ("0.4999", "low"),
    ("0.5", "medium"), ("0.9999", "medium"), ("1", "high"), ("4", "high"), ("4.01", None)])
def test_stake_boundaries(value, tier):
    assert next((b.tier for b in nenad_bands() if b.contains(D(value))), None) == tier


def test_integer_allocations_preserve_the_total():
    for target in range(201):
        assert sum(stake_targets(target, nenad_bands()).values()) == target
    assert stake_targets(56, nenad_bands()) == {"low": 19, "medium": 19, "high": 18}


def test_overlapping_bands_rejected():
    bands = nenad_bands()
    bands[0] = Band("low", D(".02"), D(".5"), 3334, True)
    with pytest.raises(ValueError, match="Overlapping"):
        validate_bands(bands)


def test_defaults_balanced_repeatable_and_profile_aware():
    accounts = [(n, f"demo{n}", n % 8 + 1) for n in range(501)]
    defaults = generate_defaults(accounts, "test")
    assert defaults == generate_defaults(list(reversed(accounts)), "test")
    for column in ("frequency", "stake", "max_minutes"):
        counts = Counter(p[column] for p in defaults.values()).values()
        assert max(counts) - min(counts) <= 1
    for account_id, _, strategy in accounts:
        p = defaults[account_id]
        assert sum(p["buy_in_weights"].values()) == 100
        if strategy >= 5:
            assert 0 <= p["post_bb_chance"] <= .2
            assert .8 <= p["run_it_twice_chance"] <= 1


def test_buyin_reweights_legal_choices_instead_of_clamping_to_maximum():
    amounts = {choose_buy_in({"20": 5, "100": 65, "200": 30}, D(1), D(40), D(150), D(500),
                             random.Random(n)) for n in range(100)}
    assert amounts == {D(100)}
    with pytest.raises(ValueError, match="Insufficient balance"):
        choose_buy_in({"100": 100}, D(1), D(40), D(200), D(20), random.Random(0))
    with pytest.raises(ValueError, match="No configured"):
        choose_buy_in({"100": 100}, D(1), D(20), D(50), D(200), random.Random(0))


def test_irregular_buyin_is_not_weighted_once_per_possible_amount():
    rng = random.Random(3)
    values = [choose_buy_in({"100": 95, "irregular": 5}, D(1), D(20), D(200), D(1000), rng)
              for _ in range(3000)]
    irregular = [x for x in values if x != 100]
    assert 100 <= len(irregular) <= 200
    assert all(41 <= x <= 99 and x % 10 != 0 for x in irregular)

