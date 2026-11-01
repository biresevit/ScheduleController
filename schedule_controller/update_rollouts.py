"""Parallel idle installations and serial customer traffic updates. This module never starts executables."""
from datetime import datetime,timedelta,timezone
from math import ceil
from .rules import Window,target_at

ACTIVE=('preparing','draining','forcing','reconciling','stopping','installing','verifying','resuming')


def recovery_target(original,target,other_entries):
    return ceil(min(original,max(0,target-other_entries))*.8)


def supports_forcing(agent_version):
    """Agents before 1.0.13 ignore 'forcing' and would leave the task waiting forever."""
    try:return tuple(int(x) for x in str(agent_version).split('.')[:3])>=(1,0,13)
    except ValueError:return False


OLD_AGENT='This PC runs an updater agent older than 1.0.13, which cannot force-stop; rerun PC setup to upgrade it.'


def unconfirmed_absence(db,site_id,machine_id,placeholder='?'):
    """Only complete, fresh seat snapshots can prove an uncertain entry absent."""
    sql="""SELECT e.EntryId,e.Status,l.FullSnapshotUtc,
        CASE WHEN l.ObserverMachineId=e.MachineId AND l.ObserverEpoch=a.WorkerEpoch AND l.FullSnapshotUtc>=DATEADD(second,-60,SYSUTCDATETIME())
          AND (SELECT COUNT(*) FROM scheduler.TableSeat s WHERE s.SiteId=e.SiteId AND s.TableId=e.TableId)=l.OccupiedSeats
          AND NOT EXISTS(SELECT 1 FROM scheduler.TableSeat s WHERE s.SiteId=e.SiteId AND s.TableId=e.TableId
              AND (s.BotAccountId=e.BotAccountId OR s.Username=b.Username)) THEN 1 ELSE 0 END AS Absent
        FROM scheduler.Entry e JOIN dbo.BotAccount b ON b.BotAccountId=e.BotAccountId
        JOIN scheduler.MachineAssignment a ON a.SiteId=e.SiteId AND a.MachineId=e.MachineId
        LEFT JOIN scheduler.LobbyTable l ON l.SiteId=e.SiteId AND l.TableId=e.TableId
        WHERE e.SiteId=? AND e.MachineId=? AND e.IsOpen=1"""
    rows=db.rows(sql.replace('?',placeholder),site_id,machine_id)
    return bool(rows) and all(r['Status']=='uncertain' and r['Absent'] for r in rows)


def tick_rollouts(db,stamp):
    now=stamp.replace(tzinfo=None)
    customers=db.rows("SELECT DISTINCT CustomerId FROM scheduler.UpdateRollout WHERE Status IN ('queued','running')")
    for customer in customers:
        cid=customer['CustomerId']
        lock=db.one("""DECLARE @r int;EXEC @r=sys.sp_getapplock @Resource=?,@LockMode='Exclusive',
            @LockOwner='Transaction',@LockTimeout=0;SELECT @r AS value""",f'UpdateRollout:customer:{cid}')
        if not lock or lock['value']<0: continue
        tick_idle_installations(db,cid,stamp)
        # Immediate rollouts update all of their PCs at once and ignore pauses
        # of other rollouts: the operator chose to interrupt traffic everywhere.
        for rollout in db.rows("""SELECT * FROM scheduler.UpdateRollout WITH(UPDLOCK,HOLDLOCK)
                WHERE CustomerId=? AND Status IN ('queued','running') AND Immediate=1 AND NotBeforeUtc<=? ORDER BY CreatedUtc""",cid,now):
            tasks=db.rows(TASKS+' ORDER BY t.Position',rollout['RolloutId'])
            if not tasks:
                db.execute("UPDATE scheduler.UpdateRollout SET Status='completed',UpdatedUtc=? WHERE RolloutId=?",now,rollout['RolloutId']);continue
            for task in tasks:advance(db,cid,rollout,task,stamp,serial=False)
        # A paused rollout blocks further traffic interruptions. Empty PCs in
        # other, unpaused rollouts have already been handled independently.
        if db.one("SELECT RolloutId FROM scheduler.UpdateRollout WHERE CustomerId=? AND Status='paused'",cid): continue
        rollout=db.one("""SELECT TOP(1) * FROM scheduler.UpdateRollout WITH(UPDLOCK,HOLDLOCK)
            WHERE CustomerId=? AND Status IN ('queued','running') AND Immediate=0 AND NotBeforeUtc<=?
              AND (EXISTS(SELECT 1 FROM scheduler.MachineUpdateTask t WHERE t.RolloutId=UpdateRollout.RolloutId AND t.IndependentInstall=0 AND t.Status NOT IN ('completed','cancelled'))
                OR NOT EXISTS(SELECT 1 FROM scheduler.MachineUpdateTask t WHERE t.RolloutId=UpdateRollout.RolloutId AND t.Status NOT IN ('completed','cancelled')))
            ORDER BY CASE WHEN Status='running' THEN 0 ELSE 1 END,CreatedUtc""",cid,now)
        if not rollout: continue
        task=db.one(TASKS.replace('SELECT','SELECT TOP(1)',1)+' ORDER BY t.Position',rollout['RolloutId'])
        if not task:
            db.execute("UPDATE scheduler.UpdateRollout SET Status='completed',UpdatedUtc=? WHERE RolloutId=?",now,rollout['RolloutId']);continue
        advance(db,cid,rollout,task,stamp,serial=True)


TASKS="""SELECT t.*,a.LastHeartbeatUtc,a.IsReady,g.LastHeartbeatUtc AS AgentHeartbeat,
        g.CurrentReleaseId,g.Enabled AS AgentEnabled,g.AppsHealthy,g.Stage AS AgentStage,g.AgentVersion
    FROM scheduler.MachineUpdateTask t JOIN scheduler.MachineAssignment a ON a.SiteId=t.SiteId AND a.MachineId=t.MachineId
    LEFT JOIN scheduler.MachineUpdateAgent g ON g.MachineId=t.MachineId AND g.SiteId=t.SiteId
    WHERE t.RolloutId=? AND t.IndependentInstall=0 AND t.Status NOT IN ('completed','cancelled')"""


def advance(db,cid,rollout,task,stamp,serial):
    """Move one traffic-carrying PC's update forward. Serial rollouts call this for one PC at a time."""
    now=stamp.replace(tzinfo=None)
    rid=rollout['RolloutId']
    tid=task['TaskId'];sid=task['SiteId'];mid=task['MachineId'];status=task['Status']
    if status in ('failed','rolled_back'):
        db.execute("UPDATE scheduler.UpdateRollout SET Status='paused',UpdatedUtc=? WHERE RolloutId=?",now,rid);return
    agent_fresh=task['AgentEnabled'] and task['AgentHeartbeat'] and task['AgentHeartbeat']>=now-timedelta(seconds=60)
    worker_fresh=task['IsReady'] and task['LastHeartbeatUtc'] and task['LastHeartbeatUtc']>=now-timedelta(seconds=180)
    # Fresh agents may install without a worker only when no application
    # release or open session/entry journal exists. Never bypass live seats.
    bootstrap=(not task['CurrentReleaseId'] and task.get('AgentStage') in ('awaiting_install','staged') and not worker_fresh
        and not db.one('SELECT TOP(1) EntryId FROM scheduler.Entry WHERE SiteId=? AND MachineId=? AND IsOpen=1',sid,mid)
        and not db.one('SELECT TOP(1) SessionId FROM scheduler.AccountSession WHERE SiteId=? AND MachineId=? AND IsOpen=1',sid,mid))
    if status=='queued':
        if not agent_fresh or not (worker_fresh or bootstrap): return
        if db.one('SELECT TaskId FROM scheduler.MachineUpdateTask WHERE MachineId=? AND StartedUtc IS NOT NULL AND CompletedUtc IS NULL AND TaskId<>?',mid,tid):return
        if serial and db.one("SELECT TaskId FROM scheduler.MachineUpdateTask WHERE StartedUtc IS NOT NULL AND CompletedUtc IS NULL AND IndependentInstall=0 AND TaskId<>? AND RolloutId IN(SELECT RolloutId FROM scheduler.UpdateRollout WHERE CustomerId=?)",tid,cid): return
        count=db.one("SELECT COUNT(*) AS n FROM scheduler.Entry WHERE SiteId=? AND MachineId=? AND IsOpen=1 AND Status IN ('seated','sitting_out','draining')",sid,mid)['n']
        db.execute("UPDATE scheduler.MachineUpdateTask SET Status='preparing',StartedUtc=?,UpdatedUtc=?,OriginalEntries=?,Detail='Downloading and verifying the release; sessions continue.' WHERE TaskId=?",now,now,count,tid)
        db.execute("UPDATE scheduler.UpdateRollout SET Status='running',UpdatedUtc=? WHERE RolloutId=?",now,rid)
    elif status=='preparing':
        if task['Staged'] and agent_fresh and (worker_fresh or bootstrap):
            count=db.one("SELECT COUNT(*) AS n FROM scheduler.Entry WHERE SiteId=? AND MachineId=? AND IsOpen=1 AND Status IN ('seated','sitting_out','draining')",sid,mid)['n']
            if bootstrap:
                db.execute("UPDATE scheduler.MachineUpdateTask SET Status='stopping',UpdatedUtc=?,OriginalEntries=?,Detail='Installing applications on a new machine.' WHERE TaskId=?",now,count,tid)
            elif rollout['Immediate'] and supports_forcing(task.get('AgentVersion')):
                # The operator explicitly chose to skip draining: open sessions/entries are cut off rather
                # than finished naturally, and are reconciled as forced (not confirmed absent) once the
                # agent reports the applications stopped. See phase='force_stopped' below.
                db.execute("UPDATE scheduler.MachineUpdateTask SET Status='forcing',UpdatedUtc=?,OriginalEntries=?,Detail='Immediate update requested: stopping applications now, regardless of open sessions.' WHERE TaskId=?",now,count,tid)
            elif rollout['Immediate']:
                db.execute("UPDATE scheduler.MachineUpdateTask SET Status='draining',UpdatedUtc=?,OriginalEntries=?,Detail=? WHERE TaskId=?",now,count,OLD_AGENT,tid)
            else:
                db.execute("UPDATE scheduler.MachineUpdateTask SET Status='draining',UpdatedUtc=?,OriginalEntries=?,Detail='Finishing one bot session at a time.' WHERE TaskId=?",now,count,tid)
            db.execute('UPDATE scheduler.MachineAssignment SET MaintenanceTaskId=? WHERE SiteId=? AND MachineId=?',tid,sid,mid)
    elif status=='forcing' and not supports_forcing(task.get('AgentVersion')):
        db.execute("UPDATE scheduler.MachineUpdateTask SET Status='draining',UpdatedUtc=?,Detail=? WHERE TaskId=?",now,OLD_AGENT,tid)
    elif status=='draining':
        counts=db.one("SELECT SUM(CASE WHEN Status='uncertain' THEN 1 ELSE 0 END) AS uncertain,COUNT(*) AS total,COUNT(DISTINCT BotAccountId) AS accounts FROM scheduler.Entry WHERE SiteId=? AND MachineId=? AND IsOpen=1",sid,mid)
        remaining=counts['total'];uncertain=counts['uncertain'] or 0
        prefix=OLD_AGENT+' ' if rollout['Immediate'] else ''
        db.execute('UPDATE scheduler.MachineUpdateTask SET Detail=? WHERE TaskId=?',prefix+f"Finishing one session at a time: {counts['accounts']} sessions, {remaining-uncertain} confirmed entries and {uncertain} unconfirmed entries remain.",tid)
        if task['Staged'] and agent_fresh and worker_fresh and unconfirmed_absence(db,sid,mid):
            db.execute("UPDATE scheduler.MachineUpdateTask SET Status='reconciling',UpdatedUtc=?,Detail='Confirmed sessions ended; reconciling absent, unconfirmed entries before installation.' WHERE TaskId=?",now,tid)
            return
        # Cancelled reservations may have a future session that never logged
        # in. Close only sessions whose full entry journal is already closed.
        db.execute("""UPDATE s SET IsOpen=0,ActualEndUtc=CASE WHEN StartedUtc>? THEN StartedUtc ELSE ? END,EndReason='software_update'
            FROM scheduler.AccountSession s WHERE s.SiteId=? AND s.MachineId=? AND s.IsOpen=1
              AND NOT EXISTS(SELECT 1 FROM scheduler.Entry e WHERE e.SessionId=s.SessionId AND e.IsOpen=1)""",now,now,sid,mid)
        open_entry=db.one('SELECT TOP(1) EntryId FROM scheduler.Entry WHERE SiteId=? AND MachineId=? AND IsOpen=1',sid,mid)
        open_session=db.one('SELECT TOP(1) SessionId FROM scheduler.AccountSession WHERE SiteId=? AND MachineId=? AND IsOpen=1',sid,mid)
        if not open_entry and not open_session and task['Staged'] and agent_fresh and worker_fresh:
            db.execute("UPDATE scheduler.MachineUpdateTask SET Status='stopping',UpdatedUtc=?,Detail='Sessions ended. Ready to restart applications.' WHERE TaskId=?",now,tid)
    elif status=='resuming':
        finish_recovery(db,task,rollout,stamp)
    # Failed or stalled updates never silently advance to the next PC.
    if status in ACTIVE and task['UpdatedUtc'] and task['UpdatedUtc']<now-timedelta(hours=4):
        db.execute("UPDATE scheduler.UpdateRollout SET Status='paused',UpdatedUtc=? WHERE RolloutId=?",now,rid)
        db.execute("UPDATE scheduler.MachineUpdateTask SET Detail='Update is taking longer than four hours. Rollout paused for review.' WHERE TaskId=?",tid)


def tick_idle_installations(db,customer_id,stamp):
    """Reserve empty machines before downloads; no live traffic is drained here."""
    now=stamp.replace(tzinfo=None)
    rollouts=db.rows("""SELECT * FROM scheduler.UpdateRollout
        WHERE CustomerId=? AND Status IN ('queued','running') AND NotBeforeUtc<=?
        ORDER BY CreatedUtc""",customer_id,now)
    for rollout in rollouts:
        rid=rollout['RolloutId']
        tasks=db.rows("""SELECT t.*,a.LastHeartbeatUtc,a.IsReady,a.MaintenanceTaskId,
            g.LastHeartbeatUtc AS AgentHeartbeat,g.CurrentReleaseId,
            g.Enabled AS AgentEnabled,g.AppsHealthy,g.Stage AS AgentStage
            FROM scheduler.MachineUpdateTask t
            JOIN scheduler.MachineAssignment a ON a.SiteId=t.SiteId AND a.MachineId=t.MachineId
            LEFT JOIN scheduler.MachineUpdateAgent g ON g.MachineId=t.MachineId AND g.SiteId=t.SiteId
            WHERE t.RolloutId=? AND t.Status NOT IN ('completed','cancelled') ORDER BY t.Position""",rid)
        if not tasks:
            db.execute("UPDATE scheduler.UpdateRollout SET Status='completed',UpdatedUtc=? WHERE RolloutId=?",now,rid)
            continue
        if any(t['Status'] in ('failed','rolled_back') for t in tasks):
            db.execute("UPDATE scheduler.UpdateRollout SET Status='paused',UpdatedUtc=? WHERE RolloutId=?",now,rid)
            continue
        for task in tasks:
            tid=task['TaskId'];sid=task['SiteId'];mid=task['MachineId'];status=task['Status']
            independent=task.get('IndependentInstall',False)
            if status!='queued' and not independent:continue
            agent_fresh=bool(task['AgentEnabled'] and task['AgentHeartbeat'] and task['AgentHeartbeat']>=now-timedelta(seconds=60))
            worker_fresh=bool(task['IsReady'] and task['LastHeartbeatUtc'] and task['LastHeartbeatUtc']>=now-timedelta(seconds=180))
            bootstrap=not task['CurrentReleaseId'] and task.get('AgentStage') in ('awaiting_install','staged')
            stopped_install=bool(task['CurrentReleaseId'] and not task['IsReady']
                and task.get('AppsHealthy') is False
                and task.get('AgentStage') in ('staged','resuming','rolled_back'))
            if status in ('queued','preparing'):
                if not agent_fresh or not (worker_fresh or bootstrap or stopped_install):continue
                # Same site lock as the planner: no new reservation can race
                # between the empty-journal check and the maintenance fence.
                db.one('SELECT SiteId FROM scheduler.Site WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=?',sid)
                if task.get('MaintenanceTaskId') not in (None,tid):continue
                if db.one('SELECT TOP(1) EntryId FROM scheduler.Entry WHERE SiteId=? AND MachineId=? AND IsOpen=1',sid,mid):continue
                if db.one('SELECT TOP(1) SessionId FROM scheduler.AccountSession WHERE SiteId=? AND MachineId=? AND IsOpen=1',sid,mid):continue
                if db.one('SELECT TaskId FROM scheduler.MachineUpdateTask WHERE MachineId=? AND StartedUtc IS NOT NULL AND CompletedUtc IS NULL AND TaskId<>?',mid,tid):continue
                if status=='queued':
                    db.execute("""UPDATE scheduler.MachineUpdateTask SET IndependentInstall=1,Status='preparing',
                        StartedUtc=?,UpdatedUtc=?,OriginalEntries=0,
                        Detail='Idle PC: downloading and verifying independently of traffic updates.' WHERE TaskId=?""",now,now,tid)
                    db.execute('UPDATE scheduler.MachineAssignment SET MaintenanceTaskId=? WHERE SiteId=? AND MachineId=?',tid,sid,mid)
                    db.execute("UPDATE scheduler.UpdateRollout SET Status='running',UpdatedUtc=? WHERE RolloutId=?",now,rid)
                elif task['Staged']:
                    db.execute("UPDATE scheduler.MachineUpdateTask SET Status='stopping',UpdatedUtc=?,Detail='No open entries or sessions. Installing applications.' WHERE TaskId=?",now,tid)
            elif status=='resuming':
                finish_recovery(db,task,rollout,stamp)
            if independent and status in ACTIVE and task['UpdatedUtc'] and task['UpdatedUtc']<now-timedelta(hours=4):
                db.execute("UPDATE scheduler.UpdateRollout SET Status='paused',UpdatedUtc=? WHERE RolloutId=?",now,rid)
                db.execute("UPDATE scheduler.MachineUpdateTask SET Detail='Update is taking longer than four hours. Rollout paused for review.' WHERE TaskId=?",tid)


def finish_recovery(db,task,rollout,stamp):
    now=stamp.replace(tzinfo=None)
    tid=task['TaskId'];sid=task['SiteId'];mid=task['MachineId']
    agent_fresh=task['AgentEnabled'] and task['AgentHeartbeat'] and task['AgentHeartbeat']>=now-timedelta(seconds=60)
    worker_fresh=task['IsReady'] and task['LastHeartbeatUtc'] and task['LastHeartbeatUtc']>=now-timedelta(seconds=180)
    site=db.one('SELECT TimeZone,SchedulingEnabled FROM scheduler.Site WHERE SiteId=?',sid)
    windows=[Window(r['Weekday'],r['StartMinute'],r['EndMinute'],r['TargetEntries']) for r in db.rows("SELECT * FROM scheduler.ScheduleWindow WHERE SiteId=? AND Format='RING'",sid)]
    target=target_at(windows,stamp,site['TimeZone']) if site['SchedulingEnabled'] else 0
    others=db.one("SELECT COUNT(*) AS n FROM scheduler.Entry WHERE SiteId=? AND MachineId<>? AND IsOpen=1 AND Status IN ('seated','sitting_out','draining')",sid,mid)['n']
    # Immediate rollouts do not hold PCs back for traffic; the planner refills them as usual.
    needed=0 if rollout.get('Immediate') else recovery_target(task['OriginalEntries'],target,others)
    actual=db.one("SELECT COUNT(*) AS n FROM scheduler.Entry WHERE SiteId=? AND MachineId=? AND IsOpen=1 AND Status IN ('seated','sitting_out','draining')",sid,mid)['n']
    db.execute('UPDATE scheduler.MachineUpdateTask SET RecoveryTarget=?,Detail=? WHERE TaskId=?',needed,f'Traffic recovery: {actual}/{needed} entries required before the next PC.',tid)
    same=str(task['CurrentReleaseId']).lower()==str(rollout['ReleaseId']).lower()
    # The agent verified versions and a fresh worker heartbeat before reporting 'resuming'. Workers publish
    # every 15-90 s under load, so stricter freshness here (or the agent's 45 s AppsHealthy flag) only made
    # completion wait for a lucky tick.
    if agent_fresh and worker_fresh and same and actual>=needed and task['UpdatedUtc']<=now-timedelta(seconds=60):
        db.execute("UPDATE scheduler.MachineUpdateTask SET Status='completed',CompletedUtc=?,UpdatedUtc=?,Detail='Healthy; traffic recovered.' WHERE TaskId=?",now,now,tid)
