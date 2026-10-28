"""Discrete event simulation around the actual preview planner; no I/O or sleeping."""
from __future__ import annotations

import heapq
import math
import random
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from itertools import count
from zoneinfo import ZoneInfo

from .planner import Account, Entry, Machine, Settings, Table, plan
from .rules import TIERS, nenad_bands, stake_targets, validate_bands


def simulate(config: dict, bands=None, currency="DEMO") -> dict:
    def bounded(key, default, minimum, maximum):
        value = config.get(key, default)
        if isinstance(value, bool) or str(value) != str(int(value)) or not minimum <= int(value) <= maximum:
            raise ValueError(f"{key} must be an integer between {minimum} and {maximum}")
        return int(value)
    target = bounded("target", 56, 0, 200)
    current = bounded("current", 20, 0, 200)
    horizon = bounded("horizon_minutes", 120, 1, 240) * 60
    tick = bounded("tick_seconds", 120, 60, 600)
    first_tick = bounded("first_tick_seconds", 0, 0, 600)
    max_joins = bounded("max_joins", 3, 1, 20)
    min_gap = bounded("min_gap_seconds", 20, 1, 300)
    max_gap = bounded("max_gap_seconds", 60, min_gap, 600)
    machine_total = bounded("machine_count", 4, 1, 8)
    capacity = bounded("machine_capacity", 20, 1, 100)
    account_total = bounded("account_count", 90, 3, 300)
    max_tables = bounded("max_tables", 6, 1, 12)
    duration_min = bounded("session_min_minutes", 60, 1, 720)
    duration_max = bounded("session_max_minutes", 240, duration_min, 1440)
    initial_remaining_min = bounded("initial_remaining_min_minutes", 15, 1, 720)
    initial_remaining_max = bounded("initial_remaining_max_minutes", 60, initial_remaining_min, 1440)
    leave_delay = bounded("finish_hand_seconds", 20, 0, 120)
    join_delay = bounded("join_latency_seconds", 5, 0, 60)
    cooldown = bounded("cooldown_minutes", 5, 0, 120) * 60
    seed = bounded("seed", 7, 0, 2**31-1)
    if current > capacity * machine_total or current > account_total * max_tables or capacity * machine_total > 400:
        raise ValueError("Initial entries exceed capacity, or total machine capacity exceeds the simulation limit of 400")
    bands = bands or nenad_bands()
    validate_bands(bands)
    zone = ZoneInfo(str(config.get("timezone", "Europe/Budapest")))
    local = datetime.fromisoformat(str(config.get("start_local", "2026-10-27T17:00")))
    if local.tzinfo is not None:
        raise ValueError("start_local must be a local date/time without an offset")
    localized = local.replace(tzinfo=zone, fold=bounded("dst_fold", 0, 0, 1))
    start = localized.astimezone(timezone.utc)
    if start.astimezone(zone).replace(tzinfo=None) != local:
        raise ValueError("Start time does not exist in this timezone because of daylight saving")
    rng = random.Random(seed)
    settings = Settings(1, bands, currency=currency, max_joins=max_joins, max_leaves=max_joins,
                        min_gap=min_gap, max_gap=max_gap, tick_seconds=tick)
    accounts = {n: Account(n, 1, TIERS[(n-1) % 3], frequency=TIERS[(n // 3) % 3],
                          max_minutes=(120, 240, 360)[(n // 9) % 3], max_tables=max_tables,
                          synthetic=True, credentials_ready=False) for n in range(1, account_total+1)}
    machines = [Machine(n, 1, capacity, start) for n in range(1, machine_total+1)]
    table_count = max(max_tables, math.ceil(max(target, current, 1) / 18) + 2)
    tables = []
    for band in bands:
        bb = ((band.minimum + band.maximum) / 2).quantize(Decimal(".0001"))
        for n in range(table_count):
            tables.append(Table(f"{band.tier}-{n+1}", 1, bb, 6, 0, start, currency=currency,
                                game="NLH" if n % 2 == 0 else "PLO4"))
    active: dict[str, Entry] = {}
    sessions: dict[int, dict] = {}
    session_history = []
    initial_sessions = []
    events, points = [], [{"seconds": 0, "entries": current, "target": target}]
    heap, serial, entry_serial = [], count(), count()
    occupied = current
    first_reached = 0 if current == target else None
    next_join = next_leave = last_planned = None
    elapsed = 0
    time_at_target = 0

    def push(seconds, kind, data):
        # Confirm departures before the next check at the same instant.
        priority = {"session_end": 0, "leave_confirm": 1, "join_confirm": 2,
                    "join": 3, "leave": 3, "cancel_join": 3, "tick": 4}[kind]
        heapq.heappush(heap, (seconds, priority, next(serial), kind, data))

    def timestamp(seconds):
        return (start + timedelta(seconds=seconds)).astimezone(zone).isoformat()

    def emit(seconds, kind, delta=0, account=None, table=None, detail=""):
        events.append({"seconds": seconds, "at": timestamp(seconds), "kind": kind, "delta": delta,
                       "entries": occupied, "account": f"Sim {account:03d}" if account else None,
                       "table": table, "detail": detail})

    def start_session(account_id, seconds, remaining=None, age=0):
        duration = remaining if remaining is not None else rng.randint(duration_min, duration_max) * 60
        session = {"account_id": account_id, "account": f"Sim {account_id:03d}", "stake": accounts[account_id].tier,
                   "started_at": timestamp(seconds-age), "planned_end_at": timestamp(seconds+duration),
                   "actual_end_at": None, "start_seconds": seconds-age, "end_seconds": seconds+duration,
                   "initial_entries": 0}
        sessions[account_id] = session
        session_history.append(session)
        accounts[account_id] = replace(accounts[account_id], session_end=start + timedelta(seconds=seconds+duration))
        push(seconds + duration, "session_end", (account_id, session))
        return session

    def allocate_initial(tier, size, remaining, age):
        eligible = [a for a in accounts.values() if a.tier == tier and a.id not in sessions]
        loads = {m.id: sum(e.machine == m.id for e in active.values()) for m in machines}
        available = [m for m in machines if capacity - loads[m.id] >= size]
        if not eligible or not available:
            raise ValueError("Initial sessions do not fit the available accounts/machine capacities")
        account = eligible[0]
        machine = min(available, key=lambda m: loads[m.id])
        suitable = [t for t in tables if t.id.startswith(tier + "-") and sum(e.table == t.id for e in active.values()) < t.seats]
        if len(suitable) < size:
            raise ValueError("Too many initial entries in one stake category")
        midnight = start.astimezone(zone).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
        accounts[account.id] = replace(account, owner_machine=machine.id,
                                       played_seconds=min(age, int((start-midnight).total_seconds())))
        for table in sorted(suitable, key=lambda t: sum(e.table == t.id for e in active.values()))[:size]:
            eid = str(next(entry_serial))
            active[eid] = Entry(eid, account.id, machine.id, table.id)
        session = start_session(account.id, 0, remaining, age)
        session["initial_entries"] = size
        initial_sessions.append(dict(session))

    manual = config.get("existing_sessions")
    if manual is not None:
        if not isinstance(manual, list) or len(manual) > account_total or not all(isinstance(r, dict) for r in manual):
            raise ValueError("Invalid existing sessions")
        if sum(int(r["entries"]) for r in manual) != current:
            raise ValueError("Existing session entries must sum to current entries")
        for row in manual:
            if row.get("stake") not in TIERS:
                raise ValueError("Invalid existing session stake")
            size, age, remaining = int(row["entries"]), int(row.get("started_minutes_ago", 0)), int(row["ends_after_minutes"])
            if not (1 <= size <= max_tables and 0 <= age <= 1440 and 0 <= remaining <= 1440 and age+remaining > 0):
                raise ValueError("Invalid existing session times or entry count")
            allocate_initial(row["stake"], size, remaining * 60, age * 60)
    else:
        for tier, remaining_count in stake_targets(current, bands).items():
            while remaining_count:
                spare = max(capacity - sum(e.machine == m.id for e in active.values()) for m in machines)
                size = min(remaining_count, max_tables, spare)
                if size <= 0:
                    raise ValueError("Insufficient initial capacity")
                allocate_initial(tier, size, rng.randint(initial_remaining_min, initial_remaining_max) * 60,
                                 rng.randint(0, duration_min) * 60)
                remaining_count -= size
    push(first_tick, "tick", None)
    while heap:
        seconds, _, _, kind, data = heapq.heappop(heap)
        if seconds > horizon:
            break
        if occupied == target:
            time_at_target += seconds - elapsed
        now = start + timedelta(seconds=seconds)
        previous = start + timedelta(seconds=elapsed)
        delta_seconds = seconds - elapsed
        if now.astimezone(zone).date() != previous.astimezone(zone).date():
            midnight = now.astimezone(zone).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
            delta_seconds = int((now-midnight).total_seconds())
            accounts = {aid: replace(a, played_seconds=0) for aid, a in accounts.items()}
        for aid in sessions:
            accounts[aid] = replace(accounts[aid], played_seconds=accounts[aid].played_seconds + delta_seconds)
        elapsed = seconds
        if kind == "tick":
            fresh_tables = []
            for table in tables:
                observed = frozenset(e.account for e in active.values() if e.table == table.id and e.status in ("seated", "draining", "sitting_out"))
                fresh_tables.append(replace(table, occupied=len(observed), observed_accounts=observed, observed=now))
            result = plan(settings, now, target, list(accounts.values()), [replace(m, heartbeat=now) for m in machines],
                          fresh_tables, list(active.values()), rng, simulation=True, next_join=next_join,
                          next_leave=next_leave, last_planned_at=last_planned)
            next_join, next_leave, last_planned = result.next_join, result.next_leave, now
            for action in result.actions:
                if action.kind == "join":
                    eid = str(next(entry_serial))
                    active[eid] = Entry(eid, action.account, action.machine, action.table, "reserved")
                    accounts[action.account] = replace(accounts[action.account], owner_machine=action.machine)
                else:
                    eid = action.entry
                    if action.kind == "leave" and eid in active:
                        active[eid] = replace(active[eid], status="draining")
                due = round((action.not_before-start).total_seconds())
                push(due, action.kind, eid)
            if result.actions:
                emit(seconds, "check", detail=f"Scheduled {sum(a.kind=='join' for a in result.actions)} joins and {sum(a.kind!='join' for a in result.actions)} departures")
            elif occupied != target:
                emit(seconds, "waiting", detail="No eligible additions: stake allocation, accounts, capacity, or session cooldown")
            push(seconds + tick, "tick", None)
        elif kind == "session_end":
            aid, expected = data
            if sessions.get(aid) is not expected:
                continue
            emit(seconds, "session_end", account=aid, detail="Planned session ended; finishing current hands")
            for eid, entry in list(active.items()):
                if entry.account != aid:
                    continue
                if entry.status in ("reserved", "joining"):
                    del active[eid]
                    emit(seconds, "cancel_join", account=aid, table=entry.table, detail="Session ended before seating")
                else:
                    active[eid] = replace(entry, status="draining")
                    push(seconds + leave_delay, "leave_confirm", eid)
        elif kind == "join":
            if data in active and active[data].status == "reserved":
                active[data] = replace(active[data], status="joining")
                push(seconds + join_delay, "join_confirm", data)
        elif kind == "join_confirm":
            if data not in active or active[data].status != "joining":
                continue
            entry = active[data]
            active[data] = replace(entry, status="seated")
            if entry.account not in sessions:
                start_session(entry.account, seconds)
            occupied += 1
            emit(seconds, "joined", 1, entry.account, entry.table, "Seat confirmed")
            points.append({"seconds": seconds, "entries": occupied, "target": target})
        elif kind in ("leave", "cancel_join"):
            if data in active:
                if kind == "cancel_join" and active[data].status == "reserved":
                    entry = active.pop(data)
                    emit(seconds, "cancel_join", account=entry.account, table=entry.table, detail="Target no longer needs this reservation")
                elif kind == "leave":
                    push(seconds + leave_delay, "leave_confirm", data)
        elif kind == "leave_confirm":
            if data not in active:
                continue
            entry = active.pop(data)
            occupied -= 1
            emit(seconds, "left", -1, entry.account, entry.table, "Seat released")
            points.append({"seconds": seconds, "entries": occupied, "target": target})
        for aid in list(sessions):
            if not any(e.account == aid for e in active.values()):
                session = sessions.pop(aid)
                session["actual_end_at"] = timestamp(seconds)
                accounts[aid] = replace(accounts[aid], owner_machine=None, session_end=None,
                                        eligible_after=now + timedelta(seconds=cooldown))
        # Clear ownership for reservations cancelled before any session began.
        for aid, account in list(accounts.items()):
            if aid not in sessions and not any(e.account == aid for e in active.values()) and account.owner_machine is not None:
                accounts[aid] = replace(account, owner_machine=None)
        if first_reached is None and occupied == target:
            first_reached = seconds
        if len(events) > 6000:
            raise ValueError("Simulation generated too many events; shorten the horizon")
    if occupied == target:
        time_at_target += horizon - elapsed
    points.append({"seconds": horizon, "entries": occupied, "target": target})
    return {"mode": "simulation", "start": start.astimezone(zone).isoformat(), "target": target,
            "initial_entries": current, "final_entries": occupied, "horizon_seconds": horizon,
            "first_reached_seconds": first_reached, "at_target_percent": round(time_at_target / horizon * 100, 2),
            "joins": sum(e["kind"] == "joined" for e in events), "departures": sum(e["kind"] == "left" for e in events),
            "points": points, "events": events, "sessions": session_history, "initial_sessions": initial_sessions,
            "assumptions": ["Uses the actual scheduler planner with simulated accounts, machines and tables.",
                "The target becomes active at the start time; the simulation shows catch-up time.",
                "Each account's entries share one session end; seats leave after the configured hand-finish delay.",
                "No network failures or depleted balances are simulated. No database or poker actions are performed."]}
