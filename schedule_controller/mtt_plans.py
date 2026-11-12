"""MTT registration plans: how many bots register for each MTT, and when (pure; shared with the backoffice).

The operator sets, per MTT, how many bots register, how many of them rebuy / re-enter and take the add-on, the
share that registers before the start (default 40%) and how many hours before the start that early registration
begins (default 2). The rest registers after the start, during late registration.

A plan belongs to the tournament's name without its date (series_name), so one plan covers every recurrence:
BluffPoker names each day's instance "Night Owl Freeroll 12/11", EvenBet repeats the same name every day.

Registrations are spread evenly: early ones over [start - early hours, start - 2 minutes], late ones over the first
80% of late registration (its end when the lobby gives it, else the first hour). Without late registration (or
when the lobby does not say it has one) every planned bot registers before the start.
"""
from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

DEFAULT_EARLY_PERCENT = 40
DEFAULT_EARLY_HOURS = 2.0
# Early registrations end this long before the start (the lobby closes normal registration at the start).
EARLY_STOP = timedelta(minutes=2)
# Late registrations are spread over this long after the start when the lobby does not say when late registration ends.
DEFAULT_LATE = timedelta(minutes=60)
# Share of the late-registration period used, so the last planned bot does not race its close.
LATE_SHARE = 0.8

# A date in a tournament name, with an optional time after it: "12/11", "05/11 03:00", "12.11.2026".
_DATE = re.compile(r"(?<![\d/.])\d{1,2}[/.]\d{1,2}(?:[/.]\d{2,4})?(?![\d/.])(?:\s+\d{1,2}:\d{2})?")


def series_name(name) -> str:
    """The tournament's name without its date, the key of its plan."""
    text = _DATE.sub(" ", str(name or ""))
    text = re.sub(r"\s+", " ", text).strip(" -–·|")
    return text[:200] or str(name or "").strip()[:200]


@dataclass(frozen=True)
class MttPlan:
    total: int
    rebuy: int = 0
    addon: int = 0
    early_percent: int = DEFAULT_EARLY_PERCENT
    early_hours: float = DEFAULT_EARLY_HOURS


def plan_from_row(row: dict) -> MttPlan:
    return MttPlan(int(row["TotalBots"]), int(row["RebuyBots"]), int(row["AddonBots"]),
                   int(row["EarlyPercent"]), float(row["EarlyHours"]))


def parse_info(value) -> dict:
    """LobbyTable.TournamentInfo: {} when the site publishes nothing."""
    if isinstance(value, dict):
        return value
    try:
        info = json.loads(value) if value else {}
    except (TypeError, ValueError):
        return {}
    return info if isinstance(info, dict) else {}


def _utc(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def late_registration(info: dict) -> bool:
    """True only when the lobby says the tournament has late registration."""
    return info.get("late_reg") is True


def split(plan: MttPlan, late: bool) -> tuple[int, int]:
    """(bots registering before the start, bots registering during late registration)."""
    if not late:
        return plan.total, 0
    early = min(plan.total, math.floor(plan.total * plan.early_percent / 100 + 0.5))
    return early, plan.total - early


def late_end(start: datetime, info: dict) -> datetime:
    end = _utc(info.get("late_reg_until"))
    return end if end is not None and end > start else start + DEFAULT_LATE


def _ramp(count: int, now: datetime, begin: datetime, end: datetime) -> int:
    if count <= 0 or now < begin:
        return 0
    if now >= end or end <= begin:
        return count
    return min(count, math.ceil(count * (now - begin) / (end - begin)))


def desired(plan: MttPlan, start: datetime, now: datetime, info: dict | None = None) -> int:
    """How many bots should have registered for this instance by `now`."""
    info = info or {}
    start, now = _utc(start), _utc(now)
    early, late = split(plan, late_registration(info))
    count = _ramp(early, now, start - timedelta(hours=plan.early_hours), start - EARLY_STOP)
    if late:
        count += _ramp(late, now, start, start + (late_end(start, info) - start) * LATE_SHARE)
    return count


def registration_window(plan: MttPlan, start: datetime, info: dict | None = None) -> tuple[datetime, datetime]:
    """When this instance's registrations happen: from the first early one to the last late one."""
    info = info or {}
    start = _utc(start)
    early, late = split(plan, late_registration(info))
    first = start - timedelta(hours=plan.early_hours) if early else start
    last = start + (late_end(start, info) - start) * LATE_SHARE if late else start - EARLY_STOP
    return first, last


def open_for(plan: MttPlan, start: datetime, now: datetime, state: str, is_open: bool, info: dict | None = None) -> bool:
    """Whether a registration can be sent now: before the start while registering, after it during late registration."""
    info = info or {}
    start, now = _utc(start), _utc(now)
    if now < start - EARLY_STOP:
        return state == "registering"
    if not late_registration(info) or not is_open or state == "completed":
        return False
    return now < late_end(start, info)


def take(left: int, remaining: int, draw: float) -> bool:
    """Whether this registration gets one of `left` rebuy / add-on places, `remaining` registrations still to come.
    Exact once every planned bot has registered; spread at random over them."""
    if left <= 0 or remaining <= 0:
        return False
    return left >= remaining or draw < left / remaining


def recurrence(starts: list[datetime]) -> dict:
    """How an MTT series repeats, from the start times of its instances."""
    times = sorted({_utc(s) for s in starts if s is not None})
    if len(times) < 2:
        return {"recurring": False, "count": len(times), "pattern": None}
    gaps = {round((b - a).total_seconds() / 3600) for a, b in zip(times, times[1:])}
    pattern = ("daily" if gaps == {24} else "weekly" if gaps == {168} else
               f"every {gaps.pop()} h" if len(gaps) == 1 else "repeats")
    return {"recurring": True, "count": len(times), "pattern": pattern}
