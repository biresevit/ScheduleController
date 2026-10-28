"""Time, stake and account defaults shared by simulation and future workers."""
from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

TIERS = ("low", "medium", "high")
FREQUENCY_WEIGHTS = {"low": 1, "medium": 2, "high": 4}


@dataclass(frozen=True)
class Window:
    weekday: int
    start_minute: int
    end_minute: int
    target: int


def validate_windows(windows: list[Window]) -> None:
    previous = None
    for w in sorted(windows, key=lambda x: (x.weekday, x.start_minute)):
        if not (0 <= w.weekday <= 6 and 0 <= w.start_minute < w.end_minute <= 1440 and w.target >= 0):
            raise ValueError("Invalid weekly window")
        if previous and previous.weekday == w.weekday and previous.end_minute > w.start_minute:
            raise ValueError("Overlapping weekly windows")
        previous = w


def target_at(windows: list[Window], now: datetime, zone: str) -> int:
    validate_windows(windows)
    require_aware(now)
    local = now.astimezone(ZoneInfo(zone))
    minute = local.hour * 60 + local.minute
    return next((w.target for w in windows if w.weekday == local.weekday()
                 and w.start_minute <= minute < w.end_minute), 0)


def require_aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("An explicit timezone is required")


def split_interval(start: datetime, end: datetime, zone: str) -> dict[str, float]:
    """One ACCOUNT interval, irrespective of its table count; accurate across DST."""
    require_aware(start)
    require_aware(end)
    start, end = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
    if end < start:
        raise ValueError("Interval ends before it starts")
    tz = ZoneInfo(zone)
    result: dict[str, float] = {}
    while start < end:
        local = start.astimezone(tz)
        midnight = datetime.combine(local.date() + timedelta(days=1), datetime.min.time(), tzinfo=tz)
        stop = min(end, midnight.astimezone(timezone.utc))
        result[local.date().isoformat()] = (stop - start).total_seconds()
        start = stop
    return result


@dataclass(frozen=True)
class Band:
    tier: str
    minimum: Decimal
    maximum: Decimal
    basis_points: int
    upper_inclusive: bool = False

    def contains(self, value: Decimal) -> bool:
        return self.minimum <= value and (value < self.maximum or self.upper_inclusive and value == self.maximum)


def validate_bands(bands: list[Band]) -> None:
    if {b.tier for b in bands} != set(TIERS) or len(bands) != 3:
        raise ValueError("Exactly one low, medium and high band is required")
    if sum(b.basis_points for b in bands) != 10000:
        raise ValueError("Stake percentages must sum to 100%")
    previous = None
    for b in sorted(bands, key=lambda x: x.minimum):
        if not (0 <= b.minimum < b.maximum and 0 <= b.basis_points <= 10000):
            raise ValueError("Invalid stake band")
        if previous and (previous.maximum > b.minimum or previous.maximum == b.minimum and previous.upper_inclusive):
            raise ValueError("Overlapping stake bands")
        previous = b


def stake_targets(total: int, bands: list[Band]) -> dict[str, int]:
    validate_bands(bands)
    if total < 0:
        raise ValueError("Target cannot be negative")
    counts = {b.tier: total * b.basis_points // 10000 for b in bands}
    order = sorted(bands, key=lambda b: (-(total * b.basis_points % 10000), TIERS.index(b.tier)))
    for b in order[:total - sum(counts.values())]:
        counts[b.tier] += 1
    return counts


def nenad_bands() -> list[Band]:
    return [Band("low", Decimal("0.02"), Decimal("0.5"), 3334),
            Band("medium", Decimal("0.5"), Decimal("1"), 3333),
            Band("high", Decimal("1"), Decimal("4"), 3333, True)]


def generate_defaults(accounts: list[tuple[int, str, int]], seed: str) -> dict[int, dict]:
    """Generate ONCE for new policies. Independent balanced assignments avoid correlation."""
    ids = sorted(a[0] for a in accounts)
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate account IDs")
    rng = random.Random(int.from_bytes(hashlib.sha256(seed.encode()).digest(), "big"))
    columns = {}
    for name, values in (("frequency", TIERS), ("stake", TIERS), ("max_minutes", (120, 240, 360))):
        assigned = [values[i % len(values)] for i in range(len(ids))]
        rng.shuffle(assigned)
        columns[name] = dict(zip(ids, assigned))
    result = {}
    for account_id, username, strategy in sorted(accounts):
        if strategy not in range(1, 9):
            raise ValueError(f"Profile {strategy} has no explicit default classification")
        strong = strategy >= 5
        weights = ({"20": 5, "30": 5, "40": 5, "100": 65, "200": 20} if strong else
                   {"20": 6.25, "30": 6.25, "40": 6.25, "50": 6.25, "100": 55, "200": 15, "irregular": 5})
        result[account_id] = {
            "username": username, "frequency": columns["frequency"][account_id],
            "stake": columns["stake"][account_id], "max_minutes": columns["max_minutes"][account_id],
            "post_bb_chance": round(rng.uniform(0, 0.2 if strong else 1), 5),
            "run_it_twice_chance": round(rng.uniform(0.8 if strong else 0, 1), 5),
            "buy_in_weights": weights,
        }
    return result


def choose_buy_in(weights: dict[str, float], bb: Decimal, minimum: Decimal, maximum: Decimal,
                  balance: Decimal, rng: random.Random, quantum: Decimal = Decimal("0.01")) -> Decimal:
    """Filter feasible choices before sampling. Never spend more than the balance."""
    if bb <= 0 or minimum < 0 or maximum < minimum or quantum <= 0:
        raise ValueError("Invalid buy-in limits")
    cap = min(maximum, balance)
    if cap < minimum:
        raise ValueError("Insufficient balance")
    candidates, probabilities = [], []
    for key, weight in weights.items():
        if not 0 <= weight < float("inf"):
            raise ValueError("Invalid buy-in weight")
        if not weight:
            continue
        values = [Decimal(n) for n in range(41, 100) if n % 10] if key == "irregular" else [Decimal(key)]
        feasible = sorted({(v * bb).quantize(quantum) for v in values
                           if minimum <= (v * bb).quantize(quantum) <= cap})
        # Keep the category's weight (5% irregular) independent of its number of amounts.
        for amount in feasible:
            candidates.append(amount)
            probabilities.append(weight / len(feasible))
    if not candidates:
        raise ValueError("No configured buy-in amount is legal")
    return rng.choices(candidates, weights=probabilities, k=1)[0]

