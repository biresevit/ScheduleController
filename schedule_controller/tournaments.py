"""Tournament registrations (SNG and MTT): pure planning, the tournament counterpart of planner.py.

A weekly-schedule target for a format is the number of bots registered in or playing that format
at once. Below target, one registration is planned at a time, paced by a random gap like ring joins:
  * accounts follow the ring rules (enabled, credentials, cooldown, session, table limit, PC);
  * a tournament fits an account when its buy-in is inside the account tier's tournament range;
  * SNG: the one with the fewest free seats, as it starts soonest; MTT: the earliest start, then the fullest;
  * a tournament whose name contains one of the site's excluded names is never chosen.
Registrations are never cancelled: once registered, a bot plays the tournament to the end.
"""
from __future__ import annotations

import random
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from .planner import Account, Entry, Machine
from .rules import FREQUENCY_WEIGHTS

FORMATS = ("SNG", "MTT")
# An MTT must still be this far from its start to be worth registering for.
MTT_MIN_LEAD = timedelta(minutes=2)


@dataclass(frozen=True)
class Tournament:
    id: str                    # lobby TableId, 'T' + EvenBet tournament id
    format: str                # 'SNG' or 'MTT'
    buy_in: Decimal
    registered: int
    max_players: int
    state: str                 # 'registering', 'running' or 'completed'
    observed: datetime
    start: datetime | None = None
    game: str = "NLH"
    name: str = ""
    open: bool = True          # lobby IsOpen: a running MTT accepts late registrations while it is open
    info: dict = field(default_factory=dict, compare=False)  # LobbyTable.TournamentInfo (mtt_plans.parse_info)


# AllowedGames value of an account that plays only SNGs and MTTs (no ring table matches it).
TOURNAMENTS_ONLY = "TOURNAMENTS"

def excluded_names(text) -> list[str]:
    """The site's excluded tournament names: one per line, blank lines ignored."""
    return [line.strip() for line in str(text or "").splitlines() if line.strip()]


def is_excluded(name, excluded) -> bool:
    """True when the tournament name contains any excluded name, ignoring case."""
    name = str(name or "").casefold()
    return any(part.casefold() in name for part in excluded)


@dataclass(frozen=True)
class Registration:
    account: int
    machine: int
    tournament: str
    not_before: datetime
    rebuy: bool | None = None  # MTT plans: whether this bot rebuys / re-enters and takes the add-on
    addon: bool | None = None


@dataclass
class TournamentPlan:
    actions: list[Registration] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)
    next_time: datetime | None = None


def fits(buy_in: Decimal, buy_in_range) -> bool:
    if buy_in_range is None:
        return False
    low, high = buy_in_range
    return low is not None and high is not None and Decimal(low) <= buy_in <= Decimal(high)


def plan_registrations(fmt: str, now: datetime, target: int, accounts: list[Account], machines: list[Machine],
                       tournaments: list[Tournament], entries: list[Entry], ranges: dict,
                       next_time: datetime | None, min_gap: int, max_gap: int, heartbeat_seconds: int,
                       freshness_seconds: int, rng: random.Random, excluded=(),
                       capacity_entries: list[Entry] | None = None) -> TournamentPlan:
    """Plan at most one registration for `fmt`.

    entries: every open entry of the site (ring and tournament), for table limits and PC capacity.
    ranges: tier -> (min buy-in, max buy-in), or None when the tier plays no tournaments.
    excluded: names (substrings, any case) of tournaments never to register for.
    """
    if fmt not in FORMATS:
        raise ValueError(f"Unknown tournament format {fmt}")
    result = TournamentPlan(next_time=next_time)
    formats = {t.id: t.format for t in tournaments}
    current = sum(1 for e in entries if formats.get(e.table) == fmt)
    if target <= current:
        result.reasons.append("target_reached")
        return result
    if next_time is not None and now < next_time.astimezone(timezone.utc):
        result.reasons.append("paced")
        return result

    def fresh(stamp: datetime, seconds: int) -> bool:
        return (now - stamp.astimezone(timezone.utc)).total_seconds() <= seconds

    # Tournaments stay NLH only: PDecision's PLO tournament play is not verified. Ring tables take all of GAMES.
    open_tournaments = [t for t in tournaments if t.format == fmt and t.state == "registering" and t.game == "NLH"
                        and not is_excluded(t.name, excluded) and fresh(t.observed, freshness_seconds)
                        and (t.registered < t.max_players if fmt == "SNG" else
                             t.start is not None and t.start.astimezone(timezone.utc) > now + MTT_MIN_LEAD)]
    if not open_tournaments:
        result.reasons.append("no_open_tournament")
        return result

    options = _options(fmt, now, accounts, machines, entries, heartbeat_seconds, capacity_entries,
                       lambda a: [t for t in open_tournaments if fits(t.buy_in, ranges.get(a.tier))
                                  and (t.game in a.games or TOURNAMENTS_ONLY in a.games)], formats)
    if not options:
        result.reasons.append("no_eligible_account")
        return result
    chosen, machine, fitting = _choose(options, rng)
    if fmt == "SNG":
        # Fewest free seats starts soonest: a 6-max at 5/6 before a 36-player SNG at 34/36.
        best = min(t.max_players - t.registered for t in fitting)
        pool = [t for t in fitting if t.max_players - t.registered == best]
    else:
        soonest = min(t.start for t in fitting)
        pool = [t for t in fitting if t.start == soonest]
        best = max(t.registered for t in pool)
        pool = [t for t in pool if t.registered == best]
    tournament = rng.choice(sorted(pool, key=lambda t: t.id))
    due = max(now, next_time.astimezone(timezone.utc)) if next_time else now
    result.actions.append(Registration(chosen, machine.id, tournament.id, due))
    result.next_time = due + timedelta(seconds=rng.uniform(min_gap, max(min_gap, max_gap)))
    return result


def _fresh(now: datetime, stamp: datetime, seconds: int) -> bool:
    return (now - stamp.astimezone(timezone.utc)).total_seconds() <= seconds


def _options(fmt, now, accounts, machines, entries, heartbeat_seconds, capacity_entries, fitting_for, formats,
             registered=None):
    """(account, machine, fitting tournaments) for every account that may register now, on every PC it may use.

    Accounts follow the ring rules (enabled, credentials, cooldown, session, table limit, one PC) and the account's
    limit of open entries of this format; PCs must be ready, fresh and below capacity."""
    ready = {m.id: m for m in machines if m.enabled and m.ready and not m.maintenance and m.capacity > 0
             and _fresh(now, m.heartbeat, heartbeat_seconds)}
    account_count = Counter(e.account for e in entries if not e.parked)
    # PC capacity: every open entry except MTT registrations whose tournament is still far away (capacity_entries).
    machine_count = Counter(e.machine for e in (entries if capacity_entries is None else capacity_entries))
    owners = {a.id: {e.machine for e in entries if e.account == a.id and not e.parked} | ({a.owner_machine} if a.owner_machine is not None else set())
              for a in accounts}
    registered = registered if registered is not None else {(e.account, e.table) for e in entries}
    # Open entries of this format per account (registered or playing), against the account's limit.
    format_count = Counter(e.account for e in entries if formats.get(e.table, fmt if str(e.table).startswith("T") else None) == fmt)
    limit = (lambda a: a.max_sng_entries) if fmt == "SNG" else (lambda a: a.max_mtt_entries)
    options = []
    for a in accounts:
        if (not a.enabled or a.synthetic or not a.credentials_ready or a.requires_reconciliation or a.manual
                or a.drain_for_update or len(owners[a.id]) > 1 or account_count[a.id] >= a.max_tables
                or format_count[a.id] >= limit(a)
                or a.eligible_after is not None and now < a.eligible_after.astimezone(timezone.utc)
                or a.session_end is not None and now >= a.session_end.astimezone(timezone.utc)):
            continue
        fitting = [t for t in fitting_for(a) if (a.id, t.id) not in registered]
        if not fitting:
            continue
        for m in ready.values():
            if machine_count[m.id] >= m.capacity or owners[a.id] and m.id not in owners[a.id]:
                continue
            options.append((a, m, fitting, machine_count[m.id] / m.capacity))
    return options


def _choose(options, rng):
    """Like ring joins: fill running sessions first, then accounts under their daily preference, weighted by
    playing frequency and favouring the least played; the least loaded of the account's PCs."""
    resuming = [o for o in options if o[0].session_end is not None]
    if resuming:
        options = resuming
    under_limit = [o for o in options if o[0].played_seconds < o[0].max_minutes * 60]
    if under_limit:
        options = under_limit
    available = {o[0].id: o[0] for o in options}
    chosen = rng.choices(list(available), weights=[FREQUENCY_WEIGHTS.get(a.frequency, 1) / (1 + a.played_seconds / 3600)
                                                   for a in available.values()], k=1)[0]
    choices = [o for o in options if o[0].id == chosen]
    best = min(choices, key=lambda o: (o[3], o[1].id))
    return chosen, best[1], best[2]


@dataclass
class MttProgress:
    """One MTT instance's registrations so far (every entry that registered, also the ones that busted)."""
    registered: int = 0
    rebuy: int = 0
    addon: int = 0
    accounts: set = field(default_factory=set)


def plan_mtt_registrations(now: datetime, plans: dict, tournaments: list[Tournament], progress: dict,
                           accounts: list[Account], machines: list[Machine], entries: list[Entry], ranges: dict,
                           next_time: datetime | None, min_gap: int, max_gap: int, heartbeat_seconds: int,
                           freshness_seconds: int, rng: random.Random, excluded=(),
                           capacity_entries: list[Entry] | None = None) -> TournamentPlan:
    """Plan at most one MTT registration from the per-MTT plans (mtt_plans).

    plans: series name -> MttPlan. progress: lobby TableId -> MttProgress.
    The MTT furthest behind its plan goes first (then the earliest start). Registrations are paced by the site's
    join gap while on plan; an MTT two or more behind registers at once (one per scheduler tick).
    Accounts whose tier buy-in range fits the MTT are preferred; the others may still play it, as the operator
    chose this MTT and its number of bots."""
    from .mtt_plans import desired, open_for, series_name, take
    result = TournamentPlan(next_time=next_time)
    behind = []
    for t in tournaments:
        if t.format != "MTT" or t.game != "NLH" or t.start is None or is_excluded(t.name, excluded):
            continue
        plan = plans.get(series_name(t.name))
        if plan is None or plan.total <= 0 or not _fresh(now, t.observed, freshness_seconds):
            continue
        if not open_for(plan, t.start, now, t.state, t.open, t.info):
            continue
        done = progress.get(t.id) or MttProgress()
        need = desired(plan, t.start, now, t.info) - done.registered
        if need > 0:
            behind.append((need, t, plan, done))
    if not behind:
        result.reasons.append("on_plan")
        return result
    behind.sort(key=lambda x: (-x[0], x[1].start, x[1].id))
    if behind[0][0] < 2 and next_time is not None and now < next_time.astimezone(timezone.utc):
        result.reasons.append("paced")
        return result
    formats = {t.id: t.format for t in tournaments}
    registered = {(e.account, e.table) for e in entries} | {(a, tid) for tid, p in progress.items() for a in p.accounts}
    for need, t, plan, done in behind:
        def fitting(a, t=t, tiered=True):
            return [t] if (t.game in a.games or TOURNAMENTS_ONLY in a.games) and (not tiered or fits(t.buy_in, ranges.get(a.tier))) else []
        options = _options("MTT", now, accounts, machines, entries, heartbeat_seconds, capacity_entries, fitting, formats, registered)
        if not options:
            options = _options("MTT", now, accounts, machines, entries, heartbeat_seconds, capacity_entries,
                               lambda a, t=t: fitting(a, t, False), formats, registered)
        if not options:
            result.reasons.append(f"no_eligible_account:{t.id}")
            continue
        chosen, machine, _ = _choose(options, rng)
        remaining = plan.total - done.registered
        rebuy = take(plan.rebuy - done.rebuy, remaining, rng.random())
        addon = take(plan.addon - done.addon, remaining, rng.random())
        due = now
        result.actions.append(Registration(chosen, machine.id, t.id, due, rebuy=rebuy, addon=addon))
        result.next_time = due + timedelta(seconds=rng.uniform(min_gap, max(min_gap, max_gap)))
        return result
    return result
