"""Transactional Azure timer scheduler. It never opens poker connections."""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import logging
import random
import uuid
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from .tournament_worker import tournament_id
from .database import connection_string
from .planner import Account, Entry, Machine, Settings, Table, plan
from .rules import Band, Window, target_at
from .tournaments import MttProgress, Tournament, excluded_names, plan_mtt_registrations, plan_registrations
from .join_failures import limit_failed_sessions

log = logging.getLogger(__name__)


def aware(value):
    return value.replace(tzinfo=timezone.utc) if value and value.tzinfo is None else value


# First PC release whose worker resumes a seat its account already holds when a join says takeover.
TAKEOVER_RELEASE = "2026.11.12.1"


def release_at_least(label, minimum):
    """Compares release labels YYYY.MM.DD.N; False for a missing or unreadable label."""
    try:
        return tuple(int(x) for x in str(label).split(".")) >= tuple(int(x) for x in minimum.split("."))
    except (TypeError, ValueError):
        return False


def takes_over_seats(site):
    """On BluffPoker a login replaces the account's earlier session, so the seat of a PC that stopped reporting can be
    taken over from another PC. Other platforms are not known to do that: their accounts are only set aside."""
    return str(site.get("Platform") or "").lower().startswith("bluffpoker")


def cancel_paused_joins(db, site_id):
    """The scheduler's join commands no PC has picked up yet (manual tasks are kept), closed with their entries."""
    return db.one("EXEC scheduler.CancelPausedJoins @SiteId=?", site_id)["n"]


# A PC claims a join for 5 minutes (ClaimUntilUtc); a join it still has not finished 10 minutes after that will not
# finish (13 Nov 2026: a worker crashed on accounts created after it started and left 131 joins claimed for hours,
# their entries counted as registered by the MTT plans and as capacity).
STUCK_CLAIM_MINUTES = 10


def expire_stuck_joins(db, site_id) -> dict:
    """Fail join commands a PC claimed and never finished; close their entries if the site never confirmed them.
    An entry already confirmed (registered or seated) keeps going: only its command is failed."""
    return db.one("""SET NOCOUNT ON;
        DECLARE @stuck TABLE(CommandId uniqueidentifier PRIMARY KEY,EntryId uniqueidentifier);
        INSERT @stuck SELECT c.CommandId,c.EntryId FROM scheduler.Command c WITH(UPDLOCK,HOLDLOCK)
            WHERE c.SiteId=? AND c.Kind='join' AND c.IsOpen=1 AND c.Status='claimed'
              AND c.ClaimUntilUtc<DATEADD(minute,-?,SYSUTCDATETIME());
        UPDATE c SET Status='failed',IsOpen=0,ResultCode='claim_expired',
            ResultText='The PC took this task but never reported a result.',ClaimToken=NULL,ClaimUntilUtc=NULL,
            UpdatedUtc=SYSUTCDATETIME()
            FROM scheduler.Command c JOIN @stuck s ON s.CommandId=c.CommandId;
        UPDATE e SET Status='failed',IsOpen=0,ClosedUtc=SYSUTCDATETIME()
            FROM scheduler.Entry e JOIN @stuck s ON s.EntryId=e.EntryId
            WHERE e.IsOpen=1 AND e.Status IN ('reserved','joining') AND e.SeatedUtc IS NULL AND e.LastConfirmedUtc IS NULL;
        DECLARE @closed int=@@ROWCOUNT;
        SELECT (SELECT COUNT(*) FROM @stuck) AS commands,@closed AS entries;""", site_id, STUCK_CLAIM_MINUTES)


class Rows:
    def __init__(self, cursor): self.cursor = cursor
    def rows(self, sql, *args):
        self.cursor.execute(sql, *args)
        names = [c[0] for c in self.cursor.description]
        return [dict(zip(names, row)) for row in self.cursor.fetchall()]
    def one(self, sql, *args): return next(iter(self.rows(sql, *args)), None)
    def execute(self, sql, *args): return self.cursor.execute(sql, *args)


# An MTT registration only takes a place on its PC from 30 minutes before the tournament starts: until then it uses
# no table, CPU or connection, and MTTs are often registered hours ahead (machine 5, 2026-11-08: 28 waiting
# registrations held 28 of its 100 entries, so ring tables stopped at ~51). When several start together the PC can
# briefly be above its maximum; the planner then joins no ring table until leaves bring it back under.
MTT_REGISTRATION_WAITING = """(e.IsParked=1 OR e.Status='joining' AND EXISTS(SELECT 1 FROM scheduler.LobbyTable w
    WHERE w.SiteId=e.SiteId AND w.TableId=e.TableId AND w.TableType='MTT' AND w.TournamentState='registering'
      AND w.StartUtc>DATEADD(minute,30,SYSUTCDATETIME())))"""

def mtt_progress(db, site_id) -> dict:
    """Registrations per MTT instance still to be planned: every entry that registered (open, or seated and since busted), with the
    plan's rebuy / add-on choices and the accounts (one registration per account and instance)."""
    progress = {}
    rebuy_accounts, addon_accounts = set(), set()
    for r in db.rows("""SELECT e.TableId,e.BotAccountId,e.TournamentRebuy,e.TournamentAddon FROM scheduler.Entry e
            JOIN scheduler.LobbyTable l ON l.SiteId=e.SiteId AND l.TableId=e.TableId
            WHERE e.SiteId=? AND l.TableType='MTT' AND l.TournamentState<>'completed'
              AND l.StartUtc>=DATEADD(day,-2,SYSUTCDATETIME()) AND e.Status<>'failed'
              AND (e.IsOpen=1 OR e.SeatedUtc IS NOT NULL)""", site_id):
        p = progress.setdefault(r["TableId"], MttProgress())
        account = r["BotAccountId"]
        key = (r["TableId"], account)
        if account not in p.accounts:
            p.registered += 1
            p.accounts.add(account)
        if r.get("TournamentRebuy") and key not in rebuy_accounts:
            p.rebuy += 1
            rebuy_accounts.add(key)
        if r.get("TournamentAddon") and key not in addon_accounts:
            p.addon += 1
            addon_accounts.add(key)
    return progress


class CloudScheduler:
    """Plans every enabled site under a per-site SQL application lock."""
    def __init__(self, connect=None, seed=1701):
        self._connect = connect
        self.seed = seed

    def connect(self):
        if self._connect: return self._connect()
        import pyodbc
        return pyodbc.connect(connection_string(), autocommit=False, timeout=15)

    @contextmanager
    def transaction(self):
        conn = self.connect()
        try:
            db = Rows(conn.cursor()); db.execute("SET XACT_ABORT ON")
            yield db
            conn.commit()
        except BaseException:
            conn.rollback(); raise
        finally:
            conn.close()

    def tick_all(self, stamp=None):
        stamp = (stamp or datetime.now(timezone.utc)).astimezone(timezone.utc)
        with self.transaction() as db:
            from .update_rollouts import tick_rollouts
            tick_rollouts(db,stamp)
            db.execute("EXEC scheduler.ExpireOfflineMachines")
            sites = [r["SiteId"] for r in db.rows("SELECT SiteId FROM scheduler.Site WHERE SchedulingEnabled=1 OR EXISTS(SELECT 1 FROM scheduler.MachineAssignment a WHERE a.SiteId=scheduler.Site.SiteId AND a.MaintenanceTaskId IS NOT NULL)")]
        results = []
        for site_id in sites:
            try: results.append(self.tick_site(site_id, stamp))
            except Exception:
                log.exception("Scheduling site %s failed", site_id)
                results.append({"site_id": site_id, "status": "failed"})
        return results

    def tick_site(self, site_id, stamp):
        with self.transaction() as db:
            lock = db.one("""DECLARE @r int; EXEC @r=sys.sp_getapplock @Resource=?,@LockMode='Exclusive',
                @LockOwner='Transaction',@LockTimeout=5000; SELECT @r AS value""", f"ScheduleController:site:{site_id}")
            if not lock or lock["value"] < 0: raise RuntimeError("Could not acquire site scheduling lock")
            site = db.one("SELECT * FROM scheduler.Site WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=?", site_id)
            maintenance=db.rows("SELECT MachineId,MaintenanceTaskId FROM scheduler.MachineAssignment WHERE SiteId=? AND MaintenanceTaskId IS NOT NULL",site_id)
            if not site or (not site["SchedulingEnabled"] and not maintenance):
                return {"site_id": site_id, "status": "disabled"}
            bands = [Band(r["Tier"], r["MinBigBlind"], r["MaxBigBlind"], r["TargetBasisPoints"], bool(r["UpperInclusive"]))
                     for r in db.rows("SELECT * FROM scheduler.StakeBand WHERE SiteId=?", site_id)]
            windows_by_format = {}
            for r in db.rows("SELECT * FROM scheduler.ScheduleWindow WHERE SiteId=?", site_id):
                windows_by_format.setdefault(r.get("Format") or "RING", []).append(
                    Window(r["Weekday"], r["StartMinute"], r["EndMinute"], r["TargetEntries"]))
            target = target_at(windows_by_format.get("RING", []), stamp, site["TimeZone"]) if site["SchedulingEnabled"] else 0
            local_day = stamp.astimezone(ZoneInfo(site["TimeZone"])).date()
            limit_failed_sessions(db, site_id)
            stuck = expire_stuck_joins(db, site_id)
            if stuck and stuck["commands"]:
                log.warning("Site %s: %s join(s) claimed by a PC but never finished were failed (%s entries closed)",
                            site_id, stuck["commands"], stuck["entries"])
            # Joins paused from the Schedule page: no new ring joins or SNG/MTT registrations, and the scheduler's
            # joins the PC has not picked up yet are cancelled. Manual tasks, leaves and session ends go on.
            paused = bool(site.get("JoinsPaused"))
            if paused:
                cancelled = cancel_paused_joins(db, site_id)
                if cancelled:
                    log.info("Joins paused on site %s: cancelled %s queued join(s)", site_id, cancelled)
            # A worker restart flags every lease on its machine for reconciliation, but only leases
            # with open entries get reconciled, and idle-lease cleanup only covers the current run.
            # Once the new worker is ready, an earlier run's lease whose bot has no open entry cannot
            # be holding a seat; without this it locks the bot out of all play indefinitely.
            released = db.execute("""DELETE l FROM scheduler.AccountLease l
                JOIN scheduler.MachineAssignment a ON a.SiteId=l.SiteId AND a.MachineId=l.MachineId
                WHERE l.SiteId=? AND l.RequiresReconciliation=1 AND a.IsReady=1 AND a.WorkerEpoch<>l.WorkerEpoch
                  AND NOT EXISTS(SELECT 1 FROM scheduler.Entry e WHERE e.BotAccountId=l.BotAccountId AND e.IsOpen=1)""",
                site_id).rowcount
            if released > 0:
                log.info("Released %s stale lease(s) left by earlier worker runs", released)
            takeover = takes_over_seats(site)
            if takeover:
                # The same for a PC the scheduler expired (no heartbeat for 180 s, its entries closed): its leases held
                # the accounts until that PC came back, for days when it never did (machine 1022, 31 accounts from
                # 2026-11-07). A login on another PC replaces the account's session there, so they are released.
                offline = db.execute("""DELETE l FROM scheduler.AccountLease l
                    JOIN scheduler.MachineAssignment a ON a.SiteId=l.SiteId AND a.MachineId=l.MachineId
                    WHERE l.SiteId=? AND l.RequiresReconciliation=1 AND a.WorkerEpoch<>l.WorkerEpoch
                      AND (a.LastHeartbeatUtc IS NULL OR a.LastHeartbeatUtc<DATEADD(second,-180,SYSUTCDATETIME()))
                      AND NOT EXISTS(SELECT 1 FROM scheduler.Entry e WHERE e.BotAccountId=l.BotAccountId AND e.IsOpen=1)""",
                    site_id).rowcount
                if offline > 0:
                    log.info("Released %s lease(s) of offline PCs", offline)
            from .mtt_sessions import unregister_ended_sngs
            unregistering = unregister_ended_sngs(db, site)
            if unregistering:
                log.info("Site %s: unregistering %s SNG registration(s) of sessions past their planned end", site_id, unregistering)
            # Sessions outlive their tables: a bot that busts, fails a join or is no longer seated
            # keeps its session and gets new joins. The session ends once its planned end has
            # passed and the planner's leaves have closed its last table.
            ended = db.execute("""UPDATE s SET IsOpen=0,EndReason=COALESCE(s.EndReason,'planned_end'),
                    ActualEndUtc=CASE WHEN s.StartedUtc>SYSUTCDATETIME() THEN s.StartedUtc ELSE SYSUTCDATETIME() END
                FROM scheduler.AccountSession s
                WHERE s.SiteId=? AND s.IsOpen=1 AND s.PlannedEndUtc<=SYSUTCDATETIME()
                  AND NOT EXISTS(SELECT 1 FROM scheduler.Entry e WHERE e.BotAccountId=s.BotAccountId AND e.IsOpen=1 AND e.IsParked=0)""",
                site_id).rowcount
            if ended > 0:
                log.info("Closed %s session(s) that reached their planned end with no open tables", ended)
            from .mtt_sessions import wake_registrations
            if site['SchedulingEnabled']:
                login_rng=random.Random(f'mtt-login:{site_id}:{stamp}')
                for _ in range(max(1,site['MaxJoinsPerTick'])):
                    if not wake_registrations(db,site,stamp,login_rng,self._session):break
                    site['NextMttLoginUtc']=db.one('SELECT NextMttLoginUtc FROM scheduler.Site WHERE SiteId=?',site_id)['NextMttLoginUtc']
            policies = db.rows("""SELECT p.*,b.IsActive,CONVERT(bit,CASE WHEN b.LoginEmail IS NOT NULL AND b.LoginPassword IS NOT NULL THEN 1 ELSE 0 END) HasLogin,COALESCE(d.SessionSeconds,0) PlayedSeconds,
                l.MachineId OwnerMachine,l.RequiresReconciliation,s.SessionId,s.PlannedEndUtc,
                s.IsManual SessionManual,s.StartedUtc SessionStart,s.MachineId SessionMachine,
                prior.ActualEndUtc PreviousEndUtc
                FROM scheduler.AccountPolicy p JOIN dbo.BotAccount b ON b.BotAccountId=p.BotAccountId
                LEFT JOIN scheduler.AccountDay d ON d.SiteId=p.SiteId AND d.BotAccountId=p.BotAccountId AND d.LocalDate=?
                LEFT JOIN scheduler.AccountLease l ON l.BotAccountId=p.BotAccountId
                LEFT JOIN scheduler.AccountSession s ON s.BotAccountId=p.BotAccountId AND s.IsOpen=1
                OUTER APPLY(SELECT TOP(1) x.ActualEndUtc FROM scheduler.AccountSession x
                    WHERE x.BotAccountId=p.BotAccountId AND x.IsOpen=0 ORDER BY x.ActualEndUtc DESC) prior
                WHERE p.SiteId=? AND p.DeletedUtc IS NULL""", local_day, site_id)
            # Tournament entries (SNG/MTT) are planned separately and never left; for ring planning
            # they only use up each account's table limit and each PC's capacity.
            tournament_entries = db.rows(f"""SELECT e.BotAccountId,e.MachineId,e.IsParked,
                    CASE WHEN {MTT_REGISTRATION_WAITING} THEN 1 ELSE 0 END AS Waiting FROM scheduler.Entry e
                JOIN scheduler.LobbyTable t ON t.SiteId=e.SiteId AND t.TableId=e.TableId
                WHERE e.SiteId=? AND e.IsOpen=1 AND t.TableType IN ('SNG','MTT')""", site_id)
            tournament_by_account = Counter(r["BotAccountId"] for r in tournament_entries if not r['IsParked'])
            tournament_by_machine = Counter(r["MachineId"] for r in tournament_entries if not r["Waiting"])
            draining_accounts=set()
            for machine in maintenance:
                update=db.one("SELECT r.Status FROM scheduler.MachineUpdateTask t JOIN scheduler.UpdateRollout r ON r.RolloutId=t.RolloutId WHERE t.TaskId=?",machine['MaintenanceTaskId'])
                if not update or update['Status']!='running':continue
                first=db.one("""SELECT TOP(1) e.BotAccountId FROM scheduler.Entry e
                    LEFT JOIN scheduler.AccountSession x ON x.SessionId=e.SessionId
                    WHERE e.SiteId=? AND e.MachineId=? AND e.IsOpen=1 AND e.Status IN ('reserved','joining','seated','sitting_out','draining')
                    ORDER BY COALESCE(x.StartedUtc,e.CreatedUtc),e.BotAccountId,e.CreatedUtc""",site_id,machine['MachineId'])
                if first: draining_accounts.add(first['BotAccountId'])
            accounts = []
            tournament_accounts = []
            policy_by_id = {}
            for p in policies:
                aid = p["BotAccountId"]; policy_by_id[aid] = p
                previous = aware(p["PreviousEndUtc"])
                manual = bool(p["SessionManual"])
                # An open session's entries must be on its machine (Entry->AccountSession FK), and a
                # session can now be open with no tables, so pin the account to the session's machine.
                owner = p["SessionMachine"] if p["SessionId"] else p["OwnerMachine"]
                eligible = previous+timedelta(minutes=p['SessionCooldownMinutes']) if previous else None
                begins = aware(p['SessionStart'])
                if p['SessionId'] and begins and begins>stamp and not manual:
                    eligible=max(eligible,begins) if eligible else begins
                account = Account(aid, site_id, p["StakeTier"], p["PlayingFrequency"], int(p["PlayedSeconds"]),
                    p["MaxPlaytimeMinutes"], p["MaxTables"], owner,
                    enabled=bool(p["IsEnabled"] and p["IsActive"]), credentials_ready=bool(p["CredentialRef"] or p.get("HasLogin")),
                    synthetic=bool(p["IsSynthetic"]), requires_reconciliation=bool(p["RequiresReconciliation"]),
                    eligible_after=eligible,
                    session_end=aware(p["PlannedEndUtc"]),drain_for_update=aid in draining_accounts,
                    manual=manual,session_start=aware(p["SessionStart"]) if manual else None,
                    max_sng_entries=p["MaxSngEntries"],max_mtt_entries=p["MaxMttEntries"],
                    games=frozenset(g for g in str(p.get("AllowedGames") or "NLH,PLO4,PLO5").split(",") if g))
                tournament_accounts.append(account)
                ring_tables = p["MaxTables"] - tournament_by_account[aid]
                accounts.append(account if ring_tables == p["MaxTables"] else
                                replace(account, max_tables=max(1, ring_tables), enabled=account.enabled and ring_tables > 0))
            # One entry limit per PC, shared by all of its poker sites: this site gets what the others leave.
            machine_rows = db.rows(f"""SELECT a.*,COALESCE(c.MaxEntries,a.MaxEntries)-(SELECT COUNT(*) FROM scheduler.Entry e
                    WHERE e.MachineId=a.MachineId AND e.SiteId<>a.SiteId AND e.IsOpen=1
                      AND NOT {MTT_REGISTRATION_WAITING}) AS SiteCapacity
                FROM scheduler.MachineAssignment a LEFT JOIN scheduler.MachineCapacity c ON c.MachineId=a.MachineId
                WHERE a.SiteId=?""", site_id)
            tournament_machines = [Machine(m["MachineId"], site_id, max(0, m["SiteCapacity"]), aware(m["LastHeartbeatUtc"] or datetime.min),
                ready=bool(m["IsReady"]), enabled=bool(m["IsEnabled"]),maintenance=bool(m["MaintenanceTaskId"])) for m in machine_rows]
            # Each PC's latest login on any site, for the per-PC gap of the scaled pace.
            last_joins = {r["MachineId"]: aware(r["LastJoin"]) for r in db.rows("""SELECT c.MachineId,MAX(c.NotBeforeUtc) LastJoin
                FROM scheduler.Command c JOIN scheduler.MachineAssignment a ON a.MachineId=c.MachineId AND a.SiteId=?
                WHERE c.Kind='join' AND c.NotBeforeUtc>=DATEADD(minute,-30,SYSUTCDATETIME()) GROUP BY c.MachineId""", site_id)}
            # PCs whose worker resumes a seat its account already holds (the takeover join), release 2026.11.12.1 on.
            releases = {r["MachineId"]: r["Label"] for r in db.rows("""SELECT u.MachineId,r.Label FROM scheduler.MachineUpdateAgent u
                JOIN scheduler.UpdateRelease r ON r.ReleaseId=u.CurrentReleaseId WHERE u.SiteId=?""", site_id)}
            machines = [replace(m, capacity=max(0, m.capacity - tournament_by_machine[m.id]), last_join=last_joins.get(m.id),
                                takeover=release_at_least(releases.get(m.id), TAKEOVER_RELEASE))
                        for m in tournament_machines]
            machine_by_id = {m["MachineId"]: m for m in machine_rows}
            lobby_rows = db.rows("SELECT * FROM scheduler.LobbyTable WHERE SiteId=?", site_id)
            table_rows = [t for t in lobby_rows if (t.get("TableType") or "RING") == "RING"]
            ring_table_ids = {t["TableId"] for t in table_rows}
            observed = {}
            for r in db.rows("""SELECT ts.TableId,ts.BotAccountId FROM scheduler.TableSeat ts
                    JOIN scheduler.LobbyTable lt ON lt.SiteId=ts.SiteId AND lt.TableId=ts.TableId
                    WHERE ts.SiteId=? AND ts.BotAccountId IS NOT NULL
                      AND lt.ObservedUtc>=DATEADD(second,-?,SYSUTCDATETIME())""",
                    site_id, site["TableFreshnessSeconds"]):
                observed.setdefault(r["TableId"], set()).add(r["BotAccountId"])
            tables = [Table(t["TableId"], site_id, t["BigBlind"], t["MaxSeats"], t["OccupiedSeats"], aware(t["ObservedUtc"]),
                currency=t["CurrencyCode"], game=t["GameType"], open=bool(t["IsOpen"]),
                observed_accounts=frozenset(observed.get(t["TableId"],()))) for t in table_rows]
            entry_rows = db.rows("""SELECT e.*,CASE WHEN EXISTS(SELECT 1 FROM scheduler.Command c
                WHERE c.EntryId=e.EntryId AND c.Kind='leave' AND c.IsOpen=1)
                THEN 'draining' ELSE e.Status END AS PlanningStatus
                FROM scheduler.Entry e WHERE e.SiteId=? AND e.IsOpen=1""", site_id)
            all_entries = [Entry(str(e["EntryId"]), e["BotAccountId"], e["MachineId"], e["TableId"], e["PlanningStatus"]) for e in entry_rows]
            entries = [e for e in all_entries if e.table in ring_table_ids]
            # Plan on the database clock, read after every input above. A tick can take tens of
            # seconds; with the start-of-tick time, heartbeats and lobby observations written during
            # the tick looked like they came from the future and the planner treated them as stale.
            stamp = aware(db.one("SELECT SYSUTCDATETIME() AS now")["now"])
            max_actions = max(site["MaxJoinsPerTick"], site["MaxLeavesPerTick"], 1)
            planning_horizon = max(120, site["MaxJoinGapSeconds"] * max_actions + 30)
            settings = Settings(site_id, bands, site["CurrencyCode"], 0 if paused else site["MaxJoinsPerTick"], site["MaxLeavesPerTick"],
                site["MinJoinGapSeconds"], site["MaxJoinGapSeconds"], tick_seconds=30,
                planning_horizon_seconds=planning_horizon, heartbeat_seconds=site["HeartbeatTimeoutSeconds"],
                freshness_seconds=site["TableFreshnessSeconds"],
                extra_table_percent=int(site.get("ExtraTableChancePercent") if site.get("ExtraTableChancePercent") is not None else 30),
                takeover_seats=takeover)
            # The scaled pace backs off to the fixed one for five minutes after the poker site rate-limits a login.
            rate_limited = db.one("""SELECT COUNT(*) AS n FROM scheduler.Command WHERE SiteId=? AND Kind='join'
                AND CreatedUtc>=DATEADD(minute,-30,SYSUTCDATETIME()) AND UpdatedUtc>=DATEADD(minute,-5,SYSUTCDATETIME())
                AND ResultText LIKE '%RATE[_]LIMIT%'""", site_id)["n"]
            if rate_limited:
                log.info("Site %s: %s login(s) rate-limited in the last 5 minutes; using the fixed join pace", site_id, rate_limited)
            lengths = [((p.get("SessionMinMinutes") or 60) + (p.get("SessionMaxMinutes") or 240)) / 2
                       for p in policies if p["IsEnabled"] and p["IsActive"]]
            settings = replace(settings, fill_minutes=0 if rate_limited else int(site.get("FillMinutes") or 0),
                session_minutes=sum(lengths) / len(lengths) if lengths else 150,
                site_joins_per_minute=int(site.get("SiteJoinsPerMinute") or 60))
            rng = random.Random(f"{self.seed}:{site_id}:{stamp:%Y%m%d%H%M%S}")
            result = plan(settings, stamp, target, accounts, machines, tables, entries, rng,
                next_join=aware(site["NextJoinUtc"]), next_leave=aware(site["NextLeaveUtc"]),
                last_planned_at=aware(site["LastReconciledUtc"]))
            if "reconciliation_cooldown" in result.reasons:
                return {"site_id": site_id, "target": target, "status": "cooldown", "actions": 0}
            for account, reason in result.manual_blocked.items():
                log.info("Manual session for account %s cannot join: %s", account, reason)
            created = 0
            any_join_proposed = False
            last_successful_join_due = None
            for action in result.actions:
                if action.kind == "join":
                    any_join_proposed = True
                    if action.takeover:
                        log.info("Site %s: account %s holds a seat at %s that no PC reports; taking it over on machine %s",
                                 site_id, action.account, action.table, action.machine)
                    if self._create_join(db, site, action, policy_by_id[action.account], machine_by_id[action.machine], rng,
                                         {"takeover": True} if action.takeover else None):
                        created += 1
                        last_successful_join_due = action.not_before
                elif action.kind == "leave":
                    if self._create_leave(db, site, action, machine_by_id[action.machine]): created += 1
                elif action.kind == "cancel_join":
                    if self._cancel_join(db, action): created += 1
            next_join = result.next_join
            if any_join_proposed:
                # A proposed join that failed to actually create (stale lease,
                # machine not ready, etc.) must not cost the site a wasted
                # min/max pacing gap. Only a confirmed join advances the anchor;
                # otherwise fall back to the pre-tick value so the next tick
                # retries with a fresh candidate immediately instead of waiting.
                next_join = last_successful_join_due if last_successful_join_due is not None else aware(site["NextJoinUtc"])
            db.execute("UPDATE scheduler.Site SET NextJoinUtc=?,NextLeaveUtc=?,LastReconciledUtc=?,UpdatedUtc=SYSUTCDATETIME() WHERE SiteId=? AND ConfigVersion=?",
                next_join, result.next_leave, stamp.replace(tzinfo=None), site_id, site["ConfigVersion"])
            tournament_results = {}
            if site["SchedulingEnabled"] and not paused:
                tournament_results = self._plan_tournaments(db, site, stamp, windows_by_format, tournament_accounts,
                    tournament_machines, lobby_rows, policy_by_id, machine_by_id, rng)
                created += sum(r["created"] for r in tournament_results.values())
            return {"site_id": site_id, "target": target, "status": "joins_paused" if paused else "planned", "actions": created,
                    "reasons": result.reasons, "deficits": result.remaining_deficits, "tournaments": tournament_results}

    def _plan_tournaments(self, db, site, stamp, windows_by_format, accounts, machines, lobby_rows, policy_by_id, machine_by_id, rng):
        """SNG and MTT registrations, each against its own weekly schedule and pacing."""
        site_id = site["SiteId"]
        ranges = {r["Tier"]: (r["TournamentMinBuyIn"], r["TournamentMaxBuyIn"]) if r.get("TournamentMinBuyIn") is not None else None
                  for r in db.rows("SELECT * FROM scheduler.StakeBand WHERE SiteId=?", site_id)}
        from .mtt_plans import parse_info, plan_from_row
        tournaments = [Tournament(t["TableId"], t["TableType"], Decimal(t["MinBuyIn"]), int(t["OccupiedSeats"]), int(t["MaxSeats"]),
                                  t.get("TournamentState") or "registering", aware(t["ObservedUtc"]), aware(t.get("StartUtc")), t["GameType"],
                                  t.get("TableName") or "", bool(t.get("IsOpen", True)), parse_info(t.get("TournamentInfo")))
                       for t in lobby_rows if t.get("TableType") in ("SNG", "MTT")]
        # MTTs follow their per-tournament plans (Schedule page, MTT tab), not a weekly schedule.
        plans = {r["SeriesName"]: plan_from_row(r) for r in db.rows("SELECT * FROM scheduler.MttPlan WHERE SiteId=?", site_id)}
        excluded = excluded_names(site.get("ExcludedTournamentNames"))
        freshness = max(site["TableFreshnessSeconds"], 3 * (site.get("LobbyRefreshSeconds") or 60))
        results = {}
        for fmt, column in (("SNG", "NextSngUtc"), ("MTT", "NextMttUtc")):
            target = target_at(windows_by_format.get(fmt, []), stamp, site["TimeZone"]) if fmt == "SNG" else None
            # Re-read open entries so a registration planned just before counts toward this one.
            rows = db.rows(f"""SELECT e.*,CASE WHEN {MTT_REGISTRATION_WAITING} THEN 1 ELSE 0 END AS Waiting
                FROM scheduler.Entry e WHERE e.SiteId=? AND e.IsOpen=1""", site_id)
            entries = [Entry(str(e["EntryId"]), e["BotAccountId"], e["MachineId"], e["TableId"], e["Status"],parked=bool(e['IsParked'])) for e in rows]
            holding = [entry for entry, row in zip(entries, rows) if not row["Waiting"]]
            if fmt == "SNG":
                plan = plan_registrations(fmt, stamp, target, accounts, machines, tournaments, entries, ranges,
                    aware(site.get(column)), site["MinJoinGapSeconds"], site["MaxJoinGapSeconds"],
                    site["HeartbeatTimeoutSeconds"], freshness, rng, excluded, capacity_entries=holding)
            else:
                plan = plan_mtt_registrations(stamp, plans, tournaments, mtt_progress(db, site_id), accounts, machines,
                    entries, ranges, aware(site.get(column)), site["MinJoinGapSeconds"], site["MaxJoinGapSeconds"],
                    site["HeartbeatTimeoutSeconds"], freshness, rng, excluded, capacity_entries=holding)
                target = len(plans)
            created = 0
            for action in plan.actions:
                join = SimpleNamespace(account=action.account, machine=action.machine, table=action.tournament,
                                       not_before=action.not_before, soft_limit_override=False)
                row=next(t for t in lobby_rows if t['TableId']==action.tournament)
                from .mtt_sessions import waits_for_mtt
                registration_only=waits_for_mtt({'table_type':fmt,'tournament_state':row.get('TournamentState'),
                                               'start':aware(row.get('StartUtc'))},stamp)
                extra = {"format": fmt, "tournament_id": tournament_id(action.tournament),'registration_only':registration_only}
                if action.rebuy is not None:
                    extra.update(rebuy=action.rebuy, addon=action.addon)
                if self._create_join(db, site, join, policy_by_id[action.account], machine_by_id[action.machine], rng, extra):
                    created += 1
            if created:
                db.execute(f"UPDATE scheduler.Site SET {column}=? WHERE SiteId=?", plan.next_time.replace(tzinfo=None), site_id)
            results[fmt] = {"target": target, "created": created, "reasons": plan.reasons}
        return results

    def _session(self, db, site_id, account_id, machine_id, epoch, policy, due, rng):
        current = db.one("SELECT SessionId FROM scheduler.AccountSession WHERE BotAccountId=? AND IsOpen=1", account_id)
        if current: return current["SessionId"]
        session_id = str(uuid.uuid4())
        minutes = rng.randint(policy["SessionMinMinutes"], policy["SessionMaxMinutes"])
        db.execute("""INSERT scheduler.AccountSession(SessionId,SiteId,BotAccountId,MachineId,WorkerEpoch,
            StartedUtc,PlannedEndUtc,IsOpen) VALUES(?,?,?,?,?,?,?,1)""", session_id, site_id, account_id,
            machine_id, epoch, due.replace(tzinfo=None), (due+timedelta(minutes=minutes)).replace(tzinfo=None))
        return session_id

    def _create_join(self, db, site, action, policy, machine, rng, extra=None):
        def refuse(reason):
            log.info("Planned join for account %s on machine %s not created: %s", action.account, action.machine, reason)
            return False
        if not machine or not machine["WorkerEpoch"]: return refuse("machine has no worker epoch")
        if machine.get("MaintenanceTaskId"): return refuse("machine is updating")
        epoch = machine["WorkerEpoch"]
        lease = db.one("SELECT * FROM scheduler.AccountLease WITH(UPDLOCK,HOLDLOCK) WHERE BotAccountId=?", action.account)
        if lease and (lease["MachineId"] != action.machine or str(lease["WorkerEpoch"]).lower() != str(epoch).lower()
                      or lease["RequiresReconciliation"]):
            return refuse(f"account is leased by machine {lease['MachineId']} (epoch match="
                          f"{str(lease['WorkerEpoch']).lower() == str(epoch).lower()}, needs reconciliation="
                          f"{bool(lease['RequiresReconciliation'])})")
        if not lease:
            db.execute("""INSERT scheduler.AccountLease(BotAccountId,SiteId,MachineId,WorkerEpoch,LeaseToken,
                LeaseUntilUtc,LastConfirmedUtc) VALUES(?,?,?,?,?,DATEADD(second,90,SYSUTCDATETIME()),SYSUTCDATETIME())""",
                action.account, site["SiteId"], action.machine, epoch, str(uuid.uuid4()))
        session_id = None if (extra or {}).get('registration_only') else self._session(db, site["SiteId"], action.account, action.machine, epoch, policy, action.not_before, rng)
        entry_id, command_id = str(uuid.uuid4()), str(uuid.uuid4())
        db.execute("""INSERT scheduler.Entry(EntryId,SiteId,BotAccountId,MachineId,WorkerEpoch,TableId,Status,
            IsOpen,CreatedUtc,SessionId,TournamentRebuy,TournamentAddon) VALUES(?,?,?,?,?,?,'reserved',1,SYSUTCDATETIME(),?,?,?)""",
            entry_id, site["SiteId"], action.account, action.machine, epoch, action.table, session_id,
            (extra or {}).get("rebuy"), (extra or {}).get("addon"))
        payload = json.dumps({"account_id": action.account, "table_id": action.table,
                              "soft_limit_override": action.soft_limit_override, **(extra or {})})
        due = action.not_before.replace(tzinfo=None)
        db.execute("""INSERT scheduler.Command(CommandId,SiteId,EntryId,MachineId,WorkerEpoch,Kind,Status,IsOpen,
            ConfigVersion,PayloadJson,NotBeforeUtc,ExpiresUtc) VALUES(?,?,?,?,?,'join','pending',1,?,?,?,?)""",
            command_id, site["SiteId"], entry_id, action.machine, epoch, site["ConfigVersion"], payload, due, due+timedelta(minutes=10))
        db.execute("INSERT scheduler.Outbox(CommandId,AvailableUtc) VALUES(?,?)", command_id, due)
        return True

    def _create_leave(self, db, site, action, machine):
        if not machine or not machine["WorkerEpoch"]: return False
        command_id, due = str(uuid.uuid4()), action.not_before.replace(tzinfo=None)
        exists = db.one("SELECT CommandId FROM scheduler.Command WHERE EntryId=? AND Kind='leave' AND IsOpen=1", action.entry)
        if exists: return False
        db.execute("""INSERT scheduler.Command(CommandId,SiteId,EntryId,MachineId,WorkerEpoch,Kind,Status,IsOpen,
            ConfigVersion,PayloadJson,NotBeforeUtc,ExpiresUtc) VALUES(?,?,?,?,?,'leave','pending',1,?,'{}',?,?)""",
            command_id, site["SiteId"], action.entry, action.machine, machine["WorkerEpoch"], site["ConfigVersion"], due, due+timedelta(minutes=10))
        db.execute("INSERT scheduler.Outbox(CommandId,AvailableUtc) VALUES(?,?)", command_id, due)
        return True

    def _cancel_join(self, db, action):
        entry = db.one("SELECT Status FROM scheduler.Entry WITH(UPDLOCK,HOLDLOCK) WHERE EntryId=? AND IsOpen=1", action.entry)
        if not entry or entry["Status"] != "reserved": return False
        db.execute("UPDATE scheduler.Command SET Status='cancelled',IsOpen=0,ResultCode='target_reduced',UpdatedUtc=SYSUTCDATETIME() WHERE EntryId=? AND Kind='join' AND IsOpen=1", action.entry)
        db.execute("UPDATE scheduler.Entry SET Status='closed',IsOpen=0,ClosedUtc=SYSUTCDATETIME() WHERE EntryId=?", action.entry)
        db.execute("UPDATE scheduler.Outbox SET SentUtc=COALESCE(SentUtc,SYSUTCDATETIME()),LastError='cancelled' WHERE CommandId IN (SELECT CommandId FROM scheduler.Command WHERE EntryId=?)", action.entry)
        return True
