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
    # Operator-created session: always gets its first ring table, more only under the occupancy target (its
    # tables count toward it), ignores the post-session cooldown and starts no earlier than session_start.
    manual: bool = False
    session_start: datetime | None = None
    # SNG / MTT entries (registered or playing) the account may hold at the same time.
    max_sng_entries: int = 1
    max_mtt_entries: int = 1


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
    # Why each manual-session account got no join option on this pass (account id -> reason).
    manual_blocked: dict[int, str] = field(default_factory=dict)


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
    # Manual sessions count toward the occupancy target like scheduled ones; only scheduled seats are trimmed.
    current = Counter(acc[e.account].tier for e in active)
    draining = Counter(acc[e.account].tier for e in active if e.status == "draining")
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

    for a in acc.values():
        if not a.manual or account_count[a.id] >= a.max_tables:
            continue
        pinned = sorted(owners[a.id])
        reason = ("disabled or missing credentials" if not a.enabled or not a.credentials_ready else
                  "lease needs reconciliation" if a.requires_reconciliation else
                  f"pinned to several machines {pinned}" if len(pinned) > 1 else
                  "session not started" if a.session_start is not None and now < a.session_start else
                  "session ended" if a.session_end is not None and now >= a.session_end else
                  f"ring target of {desired[a.tier]} {a.tier} entries reached" if account_count[a.id] and current[a.tier] >= desired[a.tier] else None)
        if reason is None and not any(not m.maintenance and machine_count[m.id] < m.capacity and (not pinned or m.id in pinned)
                                      for m in ready.values()):
            details = [f"{m.id}: enabled={m.enabled} ready={m.ready} maintenance={m.maintenance} "
                       f"heartbeat_age={(now - m.heartbeat.astimezone(timezone.utc)).total_seconds():.0f}s "
                       f"entries={machine_count[m.id]}/{m.capacity}" for m in mach.values() if m.id in pinned]
            reason = f"machine not usable ({'; '.join(details) or 'none pinned'})"
        if reason is None and not any(table_tier[t.id] == a.tier and table_count[t.id] < t.seats and (a.id, t.id) not in pairs
                                      for t in supported.values()):
            reason = f"no fresh {a.tier} table with a free seat"
        if reason:
            result.manual_blocked[a.id] = reason

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
                # a manual session always gets its first table (a scheduled seat then leaves); more only under target
                or current[a.tier] >= desired[a.tier] and not (a.manual and account_count[a.id] == 0)):
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
        # Establish one session for each available account before assigning
        # second and third tables, so new logins come before extra tables. The
        # daily playtime preference only orders accounts with equal table counts.
        least_tables = min(account_count[o[0].id] for o in options)
        options = [o for o in options if account_count[o[0].id] == least_tables]
        under_limit = [o for o in options if o[0].played_seconds < o[0].max_minutes * 60]
        if under_limit:
            options = under_limit
        # When every account is past its preference, all stay eligible; the
        # played-time weight below still favours the least played.
        # A session outlives its tables (bust, failed join, no longer seated): refill it before
        # starting a new session for another account.
        resuming = [o for o in options if o[0].session_end is not None]
        if resuming:
            options = resuming
        available = {o[0].id: o[0] for o in options}
        chosen = rng.choices(list(available), weights=[FREQUENCY_WEIGHTS[a.frequency] /
                          (1 + a.played_seconds / 3600) for a in available.values()], k=1)[0]
        choices=[o for o in options if o[0].id == chosen]
        # Two of our accounts from one PC at one table share an IP and a single point of failure: for a given
        # table, take a PC that has no account there whenever one is available. This chooses the PC, never the
        # table: avoiding the table spread a one-PC customer's bots one per table (Simo, 2026-11-08).
        by_table={}
        for o in choices:
            by_table.setdefault(o[1].id,[]).append(o)
        choices=[o for opts in by_table.values() for o in ([x for x in opts if not concentration[x[2].id, x[1].id]] or opts)]
        # A new login goes to the least loaded PC; its later tables stay there.
        if not owners[chosen]:
            load=lambda m: machine_count[m.id] / m.capacity
            lightest=min(load(o[2]) for o in choices)
            choices=[o for o in choices if load(o[2]) == lightest]
        # Fill tables that already have players (bots or humans) before opening empty ones, without making the
        # choice deterministic: a 6-max table with 2 players weighs about 40 times an empty one.
        def table_weight(o):
            _,t,m=o
            occupied=table_count[t.id]
            free_after=max(0,t.seats-occupied-1)
            capacity=max(1,m.capacity-machine_count[m.id])
            return (occupied+1)**3/(free_after+1)*capacity
        a,t,m=rng.choices(choices,weights=[table_weight(o) for o in choices],k=1)[0]
        due_join += random_gap()
        if due_join >= now + timedelta(seconds=horizon_seconds):
            break
        result.actions.append(Action("join", a.id, m.id, t.id, due_join,
                                     soft_limit_override=not bool(under_limit)))
        result.next_join = due_join
        pairs.add((a.id, t.id))
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
