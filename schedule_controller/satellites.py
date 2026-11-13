"""Satellites: a tournament whose prize is a seat (ticket) in another MTT, its target.

BluffPoker registers every ticket winner into the target itself, right after the satellite ends (Simo, 13 Nov
2026), paid with the ticket. The lobby does not name the target, so it is read from the satellite's name:
"Sat Basso Friday Main" -> "Friday Main", "Sat Alto -> Monday Classic" -> "Monday Classic"."""
from __future__ import annotations

import re
from datetime import datetime

from .mtt_plans import series_name

_PREFIX = re.compile(r"^\s*(sat|satellite)\b", re.IGNORECASE)


def _words(text: str) -> list[str]:
    return re.findall(r"[0-9a-zà-ÿ]+", text.casefold())


def is_satellite(name: str | None) -> bool:
    return bool(name and _PREFIX.match(name))


def satellite_target(name: str | None, lobby: list[dict], ended: datetime | None = None) -> dict | None:
    """The lobby row (MTT, not itself a satellite, not completed) whose series name the satellite's name ends with:
    the longest such name, then the earliest start not before the satellite's end."""
    if not is_satellite(name):
        return None
    rest = _words(_PREFIX.sub("", series_name(name), count=1))
    best = None
    for row in lobby:
        if row.get("table_type") != "MTT" or row.get("tournament_state") == "completed" or is_satellite(row.get("name")):
            continue
        target = _words(series_name(row.get("name") or ""))
        if not target or len(target) > len(rest) or rest[-len(target):] != target:
            continue
        start = row.get("start")
        if ended is not None and start is not None and row.get("tournament_state") == "registering" and start < ended:
            continue
        key = (-len(target), start.timestamp() if start else float("inf"))
        if best is None or key < best[0]:
            best = (key, row)
    return best[1] if best else None
