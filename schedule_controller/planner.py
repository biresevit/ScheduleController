"""Pure preview planner. No database writes, queue messages, logins or purchases."""
from __future__ import annotations

import random
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from .rules import Band, FREQUENCY_WEIGHTS, TIERS, require_aware, stake_targets


@dataclass(frozen=True)
class Account:
    id: int
    site: int
    tier: str
    frequency: str = "medium"
    played_seconds: int = 0
    max_minutes: int = 240
    max_tables: int = 6
    owner_machine: int | None = None
    enabled: bool = True
    credentials_ready: bool = True
    synthetic: bool = False
    requires_reconciliation: bool = False
    eligible_after: datetime | None = None
    session_end: datetime | None = None
    drain_for_update: bool = False
    # Operator-created session: plays up to max_tables outside the occupancy target,
    # ignores the post-session cooldown and starts no earlier than session_start.
    manual: bool = False
    session_start: datetime | None = None


@dataclass(frozen=True)
class Machine:
    id: int
    site: int
    capacity: int
    heartbeat: datetime
    ready: bool = True
    enabled: bool = True
    maintenance: bool = False


@dataclass(frozen=True)
class Table:
    id: str
    site: int
    big_blind: Decimal
    seats: int
    occupied: int
    observed: datetime
    currency: str = "DEMO"
    game: str = "NLH"
    open: bool = True
    observed_accounts: frozenset[int] = frozenset()


@dataclass(frozen=True)
class Entry:
    id: str
    account: int
    machine: int
    table: str
    status: str = "seated"


@dataclass(frozen=True)
class Settings:
    site: int
    bands: list[Band]
    currency: str = "DEMO"
    max_joins: int = 3
    max_leaves: int = 3
    min_gap: int = 20
    max_gap: int = 60
    tick_seconds: int = 120
    planning_horizon_seconds: int | None = None
    heartbeat_seconds: int = 60
    freshness_seconds: int = 45


@dataclass(frozen=True)
class Action:
    kind: str
    account: int
    machine: int
    table: str
    not_before: datetime
    entry: str | None = None
    soft_limit_override: bool = False


@dataclass
class Plan:
    actions: list[Action] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)
    remaining_deficits: dict[str, int] = field(default_factory=dict)
    next_join: datetime | None = None
    next_leave: datetime | None = None


def plan(settings: Settings, now: datetime, target: int, accounts: list[Account], machines: list[Machine],
         tables: list[Table], entries: list[Entry], rng: random.Random, *, simulation: bool = False,
         next_join: datetime | None = None, next_leave: datetime | None = None,
         last_planned_at: datetime | None = None) -> Plan:
    require_aware(now)
    now = now.astimezone(timezone.utc)
    horizon_seconds = settings.planning_horizon_seconds or settings.tick_seconds
    if not (0 < settings.min_gap <= settings.max_gap and settings.tick_seconds > 0 and horizon_seconds > 0
            and settings.max_joins >= 0 and settings.max_leaves >= 0):
        raise ValueError("Invalid pacing settings")
    desired = stake_targets(target, settings.bands)
    result = Plan(next_join=next_join, next_leave=next_leave)
    if last_planned_at is not None:
        require_aware(last_planned_at)
        if now < last_planned_at + timedelta(seconds=settings.tick_seconds):
            result.reasons.append("reconciliation_cooldown")
            return result
    acc = {a.id: a for a in accounts if a.site == settings.site}
    tab = {t.id: t for t in tables if t.site == settings.site}
    mach = {m.id: m for m in machines if m.site == settings.site}
    if any(a.tier not in TIERS or a.frequency not in FREQUENCY_WEIGHTS or a.played_seconds < 0
           or a.max_minutes <= 0 or a.max_tables <= 0 for a in acc.values()):
        raise ValueError("Invalid account policy")
    active = [e for e in entries if e.status not in ("closed", "failed")]
    # Snapshot repository must supply only the selected site's entries.
    if any(e.account not in acc or e.machine not in mach or e.table not in tab for e in active):
        result.reasons.append("unreconciled_or_cross_site_entry")
        return result
    pairs = {(e.account, e.table) for e in active}
    if len(pairs) != len(active):
        result.reasons.append("duplicate_entry_reservations")
        return result
    for t in tab.values():
        if not 0 <= t.occupied <= t.seats or len(t.observed_accounts) > t.occupied:
            raise ValueError("Invalid table occupancy")
        for account_id in t.observed_accounts:
            if account_id in acc and (account_id, t.id) not in pairs:
                # Absence from the task ledger is not permission to add a replacement.
                result.reasons.append("unreconciled_observed_entry")
                return result
    owners: dict[int, set[int]] = {a.id: ({a.owner_machine} if a.owner_machine is not None else set()) for a in acc.values()}
    for e in active:
        owners[e.account].add(e.machine)
    current = Counter(acc[e.account].tier for e in active if not acc[e.account].manual)
    draining = Counter(acc[e.account].tier for e in active if e.status == "draining" and not acc[e.account].manual)
    machine_count = Counter(e.machine for e in active)
    account_count = Counter(e.account for e in active)
    concentration = Counter((e.machine, e.table) for e in active)
    # Observed seats include humans/other accounts without classifying them.
    reserved_extra = Counter(e.table for e in active if e.account not in tab[e.table].observed_accounts)
    table_count = {t.id: t.occupied + reserved_extra[t.id] for t in tab.values()}

    def fresh(stamp: datetime, seconds: int) -> bool:
        require_aware(stamp)
        return timedelta(0) <= now - stamp.astimezone(timezone.utc) <= timedelta(seconds=seconds)

    ready = {m.id: m for m in mach.values() if m.enabled and m.ready and m.capacity > 0
             and fresh(m.heartbeat, settings.heartbeat_seconds)}
    supported = {t.id: t for t in tab.values() if t.open and t.game in ("NLH", "PLO4")
                 and t.currency == settings.currency and fresh(t.observed, settings.freshness_seconds)}
    table_tier = {t.id: next((b.tier for b in settings.bands if b.contains(t.big_blind)), None)
                  for t in supported.values()}
    due_leave = max(now, next_leave or now)
    planned_departures = Counter()
    planned_entry_ids = set()

    def random_gap() -> timedelta:
        """Mix short gaps and longer pauses instead of a uniform cadence."""
        fraction=rng.betavariate(.65,.65) if settings.min_gap!=settings.max_gap else 0
        return timedelta(milliseconds=round(1000*(settings.min_gap+fraction*(settings.max_gap-settings.min_gap))))

    def schedule_departure(e: Entry) -> bool:
        nonlocal due_leave
        if len(planned_entry_ids) >= settings.max_leaves:
            return False
        if e.status != "reserved" and e.machine not in ready:
            return False
        # Keep at most one tick of the previous action queued before drawing
        # another gap. This avoids filling the distant horizon with a burst or
        # repeatedly discarding long gaps until a shorter one fits.
        if due_leave>now+timedelta(seconds=settings.tick_seconds):
            return False
        candidate_due = due_leave + random_gap()
        if candidate_due >= now + timedelta(seconds=horizon_seconds):
            return False
        due_leave = candidate_due
        result.actions.append(Action("cancel_join" if e.status == "reserved" else "leave",
                                     e.account, e.machine, e.table, due_leave, e.id))
        result.next_leave = due_leave
        planned_entry_ids.add(e.id)
        planned_departures[acc[e.account].tier] += 1
        return True

    # Session rotation is independent of the occupancy target. Expired accounts
    # leave through the same bounded, randomly spaced mechanism as a target
    # reduction; replacements are considered only after departures confirm.
    expired = sorted((e for e in active if e.status in ("reserved", "seated", "sitting_out")
                      and (acc[e.account].drain_for_update or (acc[e.account].session_end is not None
                      and now >= acc[e.account].session_end.astimezone(timezone.utc)))),
                     key=lambda e: (acc[e.account].session_end or now, e.status != "reserved", e.account, e.id))
    for e in expired:
        if len(planned_entry_ids) >= settings.max_leaves:
            break
        schedule_departure(e)
    if planned_entry_ids:
        result.reasons.append("expired_sessions_draining")

    for tier in TIERS:
        excess = max(0, current[tier] - desired[tier] - draining[tier] - planned_departures[tier])
        candidates = sorted((e for e in active if acc[e.account].tier == tier and not acc[e.account].manual
                             and e.status in ("reserved", "seated", "sitting_out") and e.id not in planned_entry_ids),
                            key=lambda e: (e.status != "reserved", -acc[e.account].played_seconds, e.id))
        for e in candidates[:excess]:
            if len(planned_entry_ids) >= settings.max_leaves:
                break
            schedule_departure(e)
            # Leave capacity is retained until confirmation. Do not replace in this pass.

    due_join = max(now, next_join or now)
    for _ in range(settings.max_joins):
        if due_join>now+timedelta(seconds=settings.tick_seconds):
            break
        options = []
        for a in acc.values():
            if (not (a.enabled or simulation and a.synthetic) or a.synthetic and not simulation
                or not a.credentials_ready and not (simulation and a.synthetic)
                or a.requires_reconciliation or len(owners[a.id]) > 1 or account_count[a.id] >= a.max_tables
                or not a.manual and a.eligible_after is not None and now < a.eligible_after
                or a.manual and a.session_start is not None and now < a.session_start
                or a.session_end is not None and now >= a.session_end
                or not a.manual and current[a.tier] >= desired[a.tier]):
                continue
            for t in supported.values():
                if table_tier[t.id] != a.tier or table_count[t.id] >= t.seats or (a.id, t.id) in pairs:
                    continue
                for m in ready.values():
                    if m.maintenance or machine_count[m.id] >= m.capacity or owners[a.id] and m.id not in owners[a.id]:
                        continue
                    options.append((a, t, m))
        if not options:
            break
        manual_options = [o for o in options if o[0].manual]
        if manual_options:
            options = manual_options
        under_limit = [o for o in options if o[0].played_seconds < o[0].max_minutes * 60]
        if under_limit:
            options = under_limit
        else:
            least = min(o[0].played_seconds for o in options)
            options = [o for o in options if o[0].played_seconds == least]
        # Establish one session for each available account before assigning
        # second and third tables. This keeps idle authenticated sessions from
        # waiting for many reconciliation ticks, while frequency still decides
        # between accounts with the same number of entries.
        least_tables = min(account_count[o[0].id] for o in options)
        options = [o for o in options if account_count[o[0].id] == least_tables]
        available = {o[0].id: o[0] for o in options}
        chosen = rng.choices(list(available), weights=[FREQUENCY_WEIGHTS[a.frequency] /
                          (1 + a.played_seconds / 3600) for a in available.values()], k=1)[0]
        choices=[o for o in options if o[0].id == chosen]
        # Prefer active/populated tables without making the choice deterministic.
        # Penalising same-machine concentration limits the blast radius of one
        # machine going offline, while remaining machine capacity breaks ties.
        def table_weight(o):
            _,t,m=o
            population=(table_count[t.id]+1)**2
            spread=1+4*concentration[m.id,t.id]
            capacity=max(1,m.capacity-machine_count[m.id])
            return population*capacity/spread
        a,t,m=rng.choices(choices,weights=[table_weight(o) for o in choices],k=1)[0]
        due_join += random_gap()
        if due_join >= now + timedelta(seconds=horizon_seconds):
            break
        result.actions.append(Action("join", a.id, m.id, t.id, due_join,
                                     soft_limit_override=not bool(under_limit)))
        result.next_join = due_join
        pairs.add((a.id, t.id))
        if not a.manual:
            current[a.tier] += 1
        account_count[a.id] += 1
        machine_count[m.id] += 1
        table_count[t.id] += 1
        concentration[m.id, t.id] += 1
        owners[a.id].add(m.id)
    result.remaining_deficits = {tier: max(0, desired[tier] - current[tier]) for tier in TIERS}
    if any(result.remaining_deficits.values()):
        result.reasons.append("shortfall: pacing, stake availability, fresh telemetry, credentials or capacity")
    return result
