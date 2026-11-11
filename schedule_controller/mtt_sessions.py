"""Future MTT registrations stay offline; paced returns start real playing sessions."""
from datetime import timedelta
import json
import uuid

LOGIN_LEAD = timedelta(minutes=30)


def waits_for_mtt(table, stamp):
    start = table.get('start')
    return (table.get('table_type') == 'MTT' and table.get('tournament_state') == 'registering'
            and start is not None and start > stamp + LOGIN_LEAD)


def wake_registrations(db, site, stamp, rng, session_factory):
    """Under the site lock: schedule at most one paced return, without another registration."""
    sid = site['SiteId']
    db.execute("""UPDATE e SET IsOpen=0,Status='closed',ClosedUtc=SYSUTCDATETIME()
        FROM scheduler.Entry e JOIN scheduler.LobbyTable l ON l.SiteId=e.SiteId AND l.TableId=e.TableId
        WHERE e.SiteId=? AND e.IsOpen=1 AND e.IsParked=1 AND l.TournamentState='completed'""", sid)
    next_due = site.get('NextMttLoginUtc')
    now = stamp.replace(tzinfo=None)
    if next_due is not None and next_due > now + timedelta(seconds=30):
        return 0
    rows = db.rows("""SELECT e.*,l.StartUtc,l.TournamentState,p.MaxTables,p.SessionMinMinutes,p.SessionMaxMinutes,
            s.SessionId AS OpenSessionId,s.MachineId AS SessionMachine
        FROM scheduler.Entry e WITH(UPDLOCK,HOLDLOCK)
        JOIN scheduler.LobbyTable l ON l.SiteId=e.SiteId AND l.TableId=e.TableId
        JOIN scheduler.AccountPolicy p ON p.BotAccountId=e.BotAccountId AND p.SiteId=e.SiteId
        JOIN dbo.BotAccount b ON b.BotAccountId=e.BotAccountId
        LEFT JOIN scheduler.AccountSession s ON s.BotAccountId=e.BotAccountId AND s.IsOpen=1
        WHERE e.SiteId=? AND e.IsOpen=1 AND e.IsParked=1 AND p.IsEnabled=1 AND b.IsActive=1
          AND (l.TournamentState='running' OR l.StartUtc<=DATEADD(minute,30,?))
          AND NOT EXISTS(SELECT 1 FROM scheduler.Command c WHERE c.EntryId=e.EntryId AND c.IsOpen=1)
          AND NOT EXISTS(SELECT 1 FROM scheduler.Command c WHERE c.EntryId=e.EntryId AND c.Status='failed'
              AND c.UpdatedUtc>=DATEADD(minute,-5,SYSUTCDATETIME())
              AND JSON_VALUE(c.PayloadJson,'$.resume_registration')='true')
        ORDER BY l.StartUtc,e.CreatedUtc,e.EntryId""", sid, now)
    for row in rows:
        aid = row['BotAccountId']
        active = db.one('SELECT COUNT(*) AS n FROM scheduler.Entry WHERE BotAccountId=? AND IsOpen=1 AND IsParked=0', aid)['n']
        if active >= row['MaxTables']:
            continue
        lease = db.one('SELECT * FROM scheduler.AccountLease WITH(UPDLOCK,HOLDLOCK) WHERE BotAccountId=?', aid)
        owner = row['SessionMachine'] or (lease and lease['MachineId'])
        machines = db.rows("""SELECT a.*,COALESCE(c.MaxEntries,a.MaxEntries) AS Capacity,
                (SELECT COUNT(*) FROM scheduler.Entry x WHERE x.MachineId=a.MachineId AND x.IsOpen=1 AND x.IsParked=0) AS Used
            FROM scheduler.MachineAssignment a LEFT JOIN scheduler.MachineCapacity c ON c.MachineId=a.MachineId
            WHERE a.SiteId=? AND a.IsEnabled=1 AND a.IsReady=1 AND a.MaintenanceTaskId IS NULL
              AND COALESCE(c.IsEnabled,1)=1 AND a.LastHeartbeatUtc>=DATEADD(second,-180,SYSUTCDATETIME())
            ORDER BY a.MachineId""", sid)
        machines = [m for m in machines if m['Used'] < m['Capacity'] and (owner is None or m['MachineId'] == owner)
                    and (not lease or not lease['RequiresReconciliation']
                         and str(lease['WorkerEpoch']).lower() == str(m['WorkerEpoch']).lower())]
        if not machines:
            continue
        machine = min(machines, key=lambda m: (m['Used'] / max(1, m['Capacity']), m['MachineId']))
        mid, epoch = machine['MachineId'], machine['WorkerEpoch']
        due = max(now, next_due) if next_due else now
        if not lease:
            db.execute("""INSERT scheduler.AccountLease(BotAccountId,SiteId,MachineId,WorkerEpoch,LeaseToken,
                LeaseUntilUtc,LastConfirmedUtc) VALUES(?,?,?,?,?,DATEADD(second,90,?),SYSUTCDATETIME())""",
                aid, sid, mid, epoch, str(uuid.uuid4()), due)
        session = session_factory(db, sid, aid, mid, epoch, row, due.replace(tzinfo=stamp.tzinfo), rng)
        db.execute("""UPDATE scheduler.Entry SET MachineId=?,WorkerEpoch=?,SessionId=?,IsParked=0,
            Status='reserved' WHERE EntryId=? AND IsParked=1""", mid, epoch, session, row['EntryId'])
        cid = str(uuid.uuid4())
        payload = json.dumps({'account_id': aid, 'table_id': row['TableId'], 'format': 'MTT',
                              'resume_registration': True,'new_session':row['OpenSessionId'] is None})
        db.execute("""INSERT scheduler.Command(CommandId,SiteId,EntryId,MachineId,WorkerEpoch,Kind,Status,IsOpen,
            ConfigVersion,PayloadJson,NotBeforeUtc,ExpiresUtc) VALUES(?,?,?,?,?,'join','pending',1,?,?,?,?)""",
            cid, sid, row['EntryId'], mid, epoch, site['ConfigVersion'], payload, due, due + timedelta(minutes=30))
        db.execute('INSERT scheduler.Outbox(CommandId,AvailableUtc) VALUES(?,?)', cid, due)
        # Existing logins need no new login gap, but still take a separate scheduler tick.
        gap = rng.uniform(site['MinJoinGapSeconds'], max(site['MinJoinGapSeconds'], site['MaxJoinGapSeconds']))
        db.execute('UPDATE scheduler.Site SET NextMttLoginUtc=? WHERE SiteId=?', due + timedelta(seconds=max(1, gap)), sid)
        return 1
    return 0
