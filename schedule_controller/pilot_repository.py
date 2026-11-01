"""SQL journal and telemetry for the explicitly bounded single-PC demo pilot.

This is not the distributed queue consumer. It fences every worker with a machine
epoch and reconciles existing seats before a replacement worker becomes ready.
"""
from .sql_retry import retry_deadlocks
from contextlib import contextmanager
from datetime import datetime,timedelta,timezone
import json
import socket
import uuid
import re

from .database import connection_string

NAMES=("richard","kate","john","jeremy","mason","monica","emma","andrew")


def iso(value):
    value=utc(value)
    return value.isoformat() if value else None


# One batch: the table upsert and the seat replacement, set-based.
LOBBY_SQL="""SET NOCOUNT ON;
DECLARE @site int=:site,@machine int=:machine,@epoch uniqueidentifier=?,@tables nvarchar(max)=?,@seatTables nvarchar(max)=?,@seats nvarchar(max)=?;
DECLARE @t TABLE(TableId nvarchar(80) PRIMARY KEY,TableName nvarchar(200),GameType varchar(8),TableType varchar(4),BigBlind decimal(19,4),
    MinBuyIn decimal(19,4),MaxBuyIn decimal(19,4),MaxSeats smallint,OccupiedSeats smallint,IsOpen bit,ObservedUtc datetime2(3),FullSnapshotUtc datetime2(3));
INSERT @t SELECT * FROM OPENJSON(@tables) WITH(TableId nvarchar(80) '$.id',TableName nvarchar(200) '$.name',GameType varchar(8) '$.game',
    TableType varchar(4) '$.type',BigBlind decimal(19,4) '$.bb',MinBuyIn decimal(19,4) '$.min',MaxBuyIn decimal(19,4) '$.max',MaxSeats smallint '$.seats',
    OccupiedSeats smallint '$.occupied',IsOpen bit '$.open',ObservedUtc datetime2(3) '$.observed',FullSnapshotUtc datetime2(3) '$.snapshot');
UPDATE l SET TableName=t.TableName,GameType=t.GameType,TableType=t.TableType,BigBlind=t.BigBlind,MinBuyIn=t.MinBuyIn,MaxBuyIn=t.MaxBuyIn,
    MaxSeats=t.MaxSeats,OccupiedSeats=t.OccupiedSeats,IsOpen=t.IsOpen,ObservedUtc=t.ObservedUtc,FullSnapshotUtc=t.FullSnapshotUtc,
    ObserverEpoch=@epoch,ObserverMachineId=@machine,SnapshotSequence=l.SnapshotSequence+1
    FROM scheduler.LobbyTable l JOIN @t t ON l.SiteId=@site AND l.TableId=t.TableId;
INSERT scheduler.LobbyTable(SiteId,TableId,TableName,GameType,TableType,BigBlind,MinBuyIn,MaxBuyIn,MaxSeats,OccupiedSeats,IsOpen,
    ObservedUtc,FullSnapshotUtc,ObserverEpoch,CurrencyCode,ObserverMachineId,SnapshotSequence)
    SELECT @site,t.TableId,t.TableName,t.GameType,t.TableType,t.BigBlind,t.MinBuyIn,t.MaxBuyIn,t.MaxSeats,t.OccupiedSeats,t.IsOpen,
        t.ObservedUtc,t.FullSnapshotUtc,@epoch,'DEMO',@machine,1
    FROM @t t WHERE NOT EXISTS(SELECT 1 FROM scheduler.LobbyTable l WHERE l.SiteId=@site AND l.TableId=t.TableId);
DELETE s FROM scheduler.TableSeat s WHERE s.SiteId=@site AND s.TableId IN (SELECT CONVERT(nvarchar(80),value) FROM OPENJSON(@seatTables));
INSERT scheduler.TableSeat(SiteId,TableId,SeatIndex,NativePlayerId,Username,BotAccountId,Stack,CurrentBet,InGame,SittingOut,HandId,ObservedUtc)
    SELECT @site,TableId,SeatIndex,NativePlayerId,Username,BotAccountId,Stack,CurrentBet,InGame,SittingOut,HandId,ObservedUtc
    FROM OPENJSON(@seats) WITH(TableId nvarchar(80) '$.table',SeatIndex smallint '$.seat',NativePlayerId nvarchar(80) '$.native',
        Username nvarchar(100) '$.name',BotAccountId int '$.account',Stack decimal(19,4) '$.stack',CurrentBet decimal(19,4) '$.bet',
        InGame bit '$.in_game',SittingOut bit '$.away',HandId nvarchar(80) '$.hand',ObservedUtc datetime2(3) '$.observed');"""


def utc(value):
    return value.replace(tzinfo=None) if value else None


class DB:
    def __init__(self,c,site_id=1,machine_id=1):
        self.c=c;self.site_id=site_id;self.machine_id=machine_id
    def bind(self,sql,args):
        values=[];original=iter(args)
        def replace(match):
            values.append(self.site_id if match[0]==':site' else self.machine_id if match[0]==':machine' else next(original))
            return '?'
        rendered=re.sub(r':site\b|:machine\b|\?',replace,sql)
        return rendered,values
    def rows(self,sql,*args):
        sql,params=self.bind(sql,args)
        self.c.execute(sql,*params)
        names=[x[0] for x in self.c.description]
        return [dict(zip(names,row)) for row in self.c.fetchall()]
    def one(self,sql,*args):
        return next(iter(self.rows(sql,*args)),None)
    def execute(self,sql,*args):
        sql,params=self.bind(sql,args)
        return self.c.execute(sql,*params)


from .chat_repository import ChatRepository
from .manual_repository import ManualRepository


class Repository(ChatRepository,ManualRepository):
    def __init__(self,epoch,site_id=1,machine_id=1):
        self.epoch=epoch;self.site_id=int(site_id);self.machine_id=int(machine_id)
    @contextmanager
    def transaction(self):
        import pyodbc
        conn=pyodbc.connect(connection_string(),autocommit=False,timeout=15)
        try:
            db=DB(conn.cursor(),self.site_id,self.machine_id)
            db.execute('SET XACT_ABORT ON')
            yield db
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally: conn.close()

    @retry_deadlocks
    def claim_observer(self):
        """A lobby scan must never log in an account owned by another PC."""
        with self.transaction() as db:
            owner=db.one("SELECT WorkerEpoch FROM scheduler.MachineAssignment WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=:site AND MachineId=:machine")
            if not owner or str(owner['WorkerEpoch']).lower()!=str(self.epoch).lower():raise RuntimeError('Worker epoch lost')
            row=db.one("""SELECT TOP(1) p.BotAccountId FROM scheduler.AccountPolicy p
                JOIN dbo.BotAccount b ON b.BotAccountId=p.BotAccountId
                LEFT JOIN scheduler.AccountLease l WITH(UPDLOCK,HOLDLOCK) ON l.BotAccountId=p.BotAccountId
                WHERE p.SiteId=:site AND p.IsEnabled=1 AND p.IsSynthetic=0 AND p.DeletedUtc IS NULL
                    AND b.IsActive=1 AND l.BotAccountId IS NULL ORDER BY p.BotAccountId""")
            if row:
                db.execute("""INSERT scheduler.AccountLease(BotAccountId,SiteId,MachineId,WorkerEpoch,LeaseToken,LeaseUntilUtc,LastConfirmedUtc)
                    VALUES(?,:site,:machine,?,?,DATEADD(second,90,SYSUTCDATETIME()),SYSUTCDATETIME())""",row['BotAccountId'],self.epoch,str(uuid.uuid4()))
            return row['BotAccountId'] if row else None

    @retry_deadlocks
    def release_observer(self,account):
        with self.transaction() as db:
            db.execute("""DELETE l FROM scheduler.AccountLease l WHERE l.SiteId=:site AND l.MachineId=:machine AND l.WorkerEpoch=? AND l.BotAccountId=?
                AND NOT EXISTS(SELECT 1 FROM scheduler.Entry e WHERE e.BotAccountId=l.BotAccountId AND e.IsOpen=1)""",self.epoch,account)

    @retry_deadlocks
    def configuration(self):
        with self.transaction() as db:
            site=db.one("SELECT s.* FROM scheduler.Site s JOIN scheduler.MachineAssignment a ON a.SiteId=s.SiteId JOIN dbo.CustomerMachine m ON m.MachineId=a.MachineId AND m.CustomerId=s.CustomerId AND m.Casino=s.Casino WHERE s.SiteId=:site AND a.MachineId=:machine AND m.IsActive=1 AND s.Casino='Evenbet'")
            if not site: raise RuntimeError('Registered Evenbet machine assignment not found')
            accounts=db.rows("SELECT p.*,b.Username,b.IsActive FROM scheduler.AccountPolicy p JOIN dbo.BotAccount b ON b.BotAccountId=p.BotAccountId WHERE p.SiteId=:site AND p.IsSynthetic=0 AND p.DeletedUtc IS NULL")
            if not accounts: raise RuntimeError('No real accounts configured for this site')
            from zoneinfo import ZoneInfo
            today=datetime.now(ZoneInfo(site['TimeZone'])).date()
            for a in accounts:
                row=db.one("SELECT SessionSeconds FROM scheduler.AccountDay WHERE SiteId=:site AND BotAccountId=? AND LocalDate=?",a['BotAccountId'],today)
                a['PlayedSeconds']=row['SessionSeconds'] if row else 0
            return {'site':site,'accounts':accounts,'bands':db.rows("SELECT * FROM scheduler.StakeBand WHERE SiteId=:site"),
                'windows':db.rows("SELECT * FROM scheduler.ScheduleWindow WHERE SiteId=:site AND Format='RING'"),
                'machine':db.one("SELECT * FROM scheduler.MachineAssignment WHERE SiteId=:site AND MachineId=:machine")}

    @retry_deadlocks
    def start(self,capacity):
        with self.transaction() as db:
            machine=db.one("SELECT * FROM scheduler.MachineAssignment WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=:site AND MachineId=:machine")
            if not machine: raise RuntimeError('Machine assignment not found')
            if machine['LastHeartbeatUtc'] and machine['IsReady'] and machine['LastHeartbeatUtc']>datetime.now(timezone.utc).replace(tzinfo=None)-timedelta(seconds=90):
                raise RuntimeError('Machine has a live worker')
            if db.one("SELECT TOP(1) EntryId FROM scheduler.Entry WHERE SiteId=:site AND IsOpen=1"):
                raise RuntimeError('Open entries require reconciliation before the pilot')
            db.execute("UPDATE scheduler.MachineAssignment SET MaxEntries=?,IsReady=1,IsEnabled=1,WorkerEpoch=?,LastHeartbeatUtc=SYSUTCDATETIME(),ControllerVersion='local-pilot-0.1',DecisionUrl='http://127.0.0.1:5005' WHERE MachineId=:machine AND SiteId=:site",capacity,self.epoch)
            db.execute("UPDATE backoffice.MachineMetadata SET HostName=?,LastAgentUtc=SYSUTCDATETIME(),Version=Version+1 WHERE MachineId=:machine",socket.gethostname())

    @retry_deadlocks
    def start_cloud_worker(self):
        """Fence the old epoch and return every entry that needs live reconciliation."""
        with self.transaction() as db:
            machine=db.one("SELECT * FROM scheduler.MachineAssignment WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=:site AND MachineId=:machine")
            if not machine: raise RuntimeError('Machine assignment not found')
            cutoff=datetime.now(timezone.utc).replace(tzinfo=None)-timedelta(seconds=90)
            if machine['LastHeartbeatUtc'] and machine['IsReady'] and machine['LastHeartbeatUtc']>cutoff:
                raise RuntimeError('Machine has a live worker')
            entries=db.rows("""SELECT e.EntryId,e.BotAccountId,b.Username,e.TableId,e.Status,e.BuyInAmount,
                    e.CreatedUtc,e.SeatedUtc,e.LastConfirmedUtc,e.SessionId,s.StartedUtc,s.PlannedEndUtc
                FROM scheduler.Entry e WITH(UPDLOCK,HOLDLOCK)
                JOIN dbo.BotAccount b ON b.BotAccountId=e.BotAccountId
                LEFT JOIN scheduler.AccountSession s ON s.SessionId=e.SessionId
                WHERE e.SiteId=:site AND e.MachineId=:machine AND e.IsOpen=1 ORDER BY e.BotAccountId,e.TableId""")
            # Commands claimed by the dead epoch can never safely be replayed.
            # The Azure planner will create fresh leave/join commands after the
            # observed seats have been adopted or closed.
            db.execute("""UPDATE c SET Status='cancelled',IsOpen=0,ResultCode='worker_restarted',
                    ClaimToken=NULL,ClaimUntilUtc=NULL,UpdatedUtc=SYSUTCDATETIME()
                FROM scheduler.Command c JOIN scheduler.Entry e ON e.EntryId=c.EntryId
                WHERE e.SiteId=:site AND e.MachineId=:machine AND e.IsOpen=1 AND c.IsOpen=1""")
            db.execute("UPDATE scheduler.Entry SET Status='uncertain' WHERE SiteId=:site AND MachineId=:machine AND IsOpen=1")
            db.execute("""UPDATE scheduler.AccountLease SET RequiresReconciliation=1,
                LeaseUntilUtc=DATEADD(second,90,SYSUTCDATETIME()) WHERE SiteId=:site AND MachineId=:machine""")
            db.execute("""UPDATE scheduler.MachineAssignment SET IsReady=0,
                WorkerEpoch=?,LastHeartbeatUtc=SYSUTCDATETIME(),ControllerVersion='poker-controller-cloud-worker-0.1',
                DecisionUrl='http://127.0.0.1:5005' WHERE MachineId=:machine AND SiteId=:site""",self.epoch)
            db.execute("UPDATE backoffice.MachineMetadata SET HostName=?,LastAgentUtc=SYSUTCDATETIME(),Version=Version+1 WHERE MachineId=:machine",socket.gethostname())
            return entries

    @retry_deadlocks
    def adopt_reconciled_entry(self,entry_id,status):
        if status not in ('seated','sitting_out'):
            raise ValueError('Invalid adopted entry status')
        with self.transaction() as db:
            entry=db.one("""SELECT EntryId,BotAccountId,SessionId FROM scheduler.Entry WITH(UPDLOCK,HOLDLOCK)
                WHERE SiteId=:site AND MachineId=:machine AND EntryId=? AND IsOpen=1 AND Status='uncertain'""",entry_id)
            if not entry: raise RuntimeError('Expected one uncertain entry to adopt')
            db.execute("""UPDATE scheduler.Entry SET WorkerEpoch=?,Status=?,LastConfirmedUtc=SYSUTCDATETIME()
                WHERE EntryId=?""",self.epoch,status,entry_id)
            db.execute("""UPDATE scheduler.AccountLease SET WorkerEpoch=?,LeaseToken=?,
                LeaseUntilUtc=DATEADD(second,90,SYSUTCDATETIME()),LastConfirmedUtc=SYSUTCDATETIME(),RequiresReconciliation=0
                WHERE SiteId=:site AND BotAccountId=?""",self.epoch,str(uuid.uuid4()),entry['BotAccountId'])
            if entry['SessionId']:
                db.execute("UPDATE scheduler.AccountSession SET WorkerEpoch=? WHERE SessionId=? AND IsOpen=1",self.epoch,entry['SessionId'])

    @retry_deadlocks
    def close_reconciled_entry(self,entry_id):
        with self.transaction() as db:
            entry=db.one("""SELECT EntryId,BotAccountId,SessionId FROM scheduler.Entry WITH(UPDLOCK,HOLDLOCK)
                WHERE SiteId=:site AND MachineId=:machine AND EntryId=? AND IsOpen=1 AND Status='uncertain'""",entry_id)
            if not entry: raise RuntimeError('Expected one uncertain entry to close')
            db.execute("""UPDATE scheduler.Entry SET Status='closed',IsOpen=0,ClosedUtc=SYSUTCDATETIME(),
                LastConfirmedUtc=SYSUTCDATETIME() WHERE EntryId=?""",entry_id)
            remaining=db.one("SELECT TOP(1) EntryId FROM scheduler.Entry WHERE SiteId=:site AND BotAccountId=? AND IsOpen=1",entry['BotAccountId'])
            if not remaining:
                # The session stays open; the scheduler rejoins it until its planned end.
                db.execute("DELETE scheduler.AccountLease WHERE SiteId=:site AND BotAccountId=?",entry['BotAccountId'])

    @retry_deadlocks
    def finish_cloud_worker_reconciliation(self):
        with self.transaction() as db:
            machine=db.one("SELECT WorkerEpoch FROM scheduler.MachineAssignment WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=:site AND MachineId=:machine")
            if not machine or str(machine['WorkerEpoch']).lower()!=str(self.epoch).lower():
                raise RuntimeError('Worker epoch lost during reconciliation')
            if db.one("""SELECT TOP(1) EntryId FROM scheduler.Entry WHERE SiteId=:site AND MachineId=:machine
                    AND IsOpen=1 AND (Status='uncertain' OR WorkerEpoch<>?)""",self.epoch):
                raise RuntimeError('Entry reconciliation is incomplete')
            db.execute("""UPDATE scheduler.MachineAssignment SET IsReady=1,LastHeartbeatUtc=SYSUTCDATETIME()
                WHERE SiteId=:site AND MachineId=:machine AND WorkerEpoch=?""",self.epoch)

    @retry_deadlocks
    def claim_due(self, limit=10):
        """Atomically claim due Azure-planned commands for this worker epoch."""
        token=str(uuid.uuid4())
        with self.transaction() as db:
            rows=db.rows("""SELECT TOP (?) c.CommandId,c.EntryId,c.Kind,c.ConfigVersion,c.NotBeforeUtc,
                    e.BotAccountId,e.TableId,e.Status AS EntryStatus,e.CreatedUtc,e.SessionId,
                    s.StartedUtc,s.PlannedEndUtc
                FROM scheduler.Command c WITH(UPDLOCK,READPAST,ROWLOCK)
                JOIN scheduler.Entry e ON e.SiteId=c.SiteId AND e.EntryId=c.EntryId
                LEFT JOIN scheduler.AccountSession s ON s.SessionId=e.SessionId
                WHERE c.SiteId=:site AND c.MachineId=:machine AND c.WorkerEpoch=? AND c.IsOpen=1
                    AND c.Status='pending' AND c.NotBeforeUtc<=SYSUTCDATETIME()
                    AND c.ExpiresUtc>SYSUTCDATETIME()
                ORDER BY c.NotBeforeUtc,c.CreatedUtc""",limit,self.epoch)
            for row in rows:
                db.execute("""UPDATE scheduler.Command SET Status='claimed',ClaimToken=?,
                    ClaimUntilUtc=DATEADD(minute,5,SYSUTCDATETIME()),AttemptCount=AttemptCount+1,
                    UpdatedUtc=SYSUTCDATETIME() WHERE CommandId=? AND Status='pending' AND IsOpen=1""",
                    token,row['CommandId'])
                db.execute("UPDATE scheduler.Outbox SET SentUtc=COALESCE(SentUtc,SYSUTCDATETIME()),AttemptCount=AttemptCount+1 WHERE CommandId=?",row['CommandId'])
                row['ClaimToken']=token
            return rows

    @retry_deadlocks
    def stop_cloud_worker(self):
        with self.transaction() as db:
            db.execute("""UPDATE scheduler.MachineAssignment SET IsReady=0,LastHeartbeatUtc=SYSUTCDATETIME()
                WHERE SiteId=:site AND MachineId=:machine AND WorkerEpoch=?""",self.epoch)

    @retry_deadlocks
    def publish_machine_telemetry(self,metrics):
        with self.transaction() as db:
            owned=db.one("SELECT WorkerEpoch FROM scheduler.MachineAssignment WHERE SiteId=:site AND MachineId=:machine")
            if not owned or str(owned['WorkerEpoch']).lower()!=str(self.epoch).lower():
                raise RuntimeError('Worker epoch lost')
            db.execute("""INSERT scheduler.MachineTelemetry(
                SiteId,MachineId,CpuPercent,MemoryPercent,DiskPercent,DiskFreeBytes,DiskTotalBytes,
                ActiveEntries,PokerControllerVersion,PDecisionVersion) VALUES(:site,:machine,?,?,?,?,?,(SELECT COUNT(*) FROM scheduler.Entry
                    WHERE SiteId=:site AND MachineId=:machine AND IsOpen=1
                      AND Status IN ('seated','sitting_out','draining')),?,?)""",metrics['cpu_percent'],metrics['memory_percent'],
                metrics['disk_percent'],metrics['disk_free_bytes'],metrics['disk_total_bytes'],
                metrics.get('poker_controller_version'),metrics.get('pdecision_version'))
            db.execute("DELETE scheduler.MachineTelemetry WHERE MachineId=:machine AND SampledUtc<DATEADD(day,-90,SYSUTCDATETIME())")

    @retry_deadlocks
    def prepare_schedule(self,start,end,target,high_only):
        from zoneinfo import ZoneInfo
        saved=self.configuration()
        zone=ZoneInfo(saved['site']['TimeZone'])
        windows=[]
        cursor=start.astimezone(zone).replace(second=0,microsecond=0)
        stop=end.astimezone(zone)+timedelta(minutes=1)
        while cursor<stop:
            midnight=(cursor+timedelta(days=1)).replace(hour=0,minute=0,second=0,microsecond=0)
            until=min(midnight,stop)
            windows.append({'Weekday':cursor.weekday(),'StartMinute':cursor.hour*60+cursor.minute,
                'EndMinute':1440 if until==midnight else until.hour*60+until.minute,'TargetEntries':target})
            cursor=until
        with self.transaction() as db:
            for a in saved['accounts']:
                db.execute("UPDATE scheduler.AccountPolicy SET IsEnabled=1,CredentialRef=? WHERE SiteId=:site AND BotAccountId=?",'local-demo:'+a['Username'],a['BotAccountId'])
            if high_only:
                for a in saved['accounts']:
                    db.execute("UPDATE scheduler.AccountPolicy SET StakeTier='high' WHERE BotAccountId=? AND SiteId=:site",a['BotAccountId'])
                db.execute("UPDATE scheduler.StakeBand SET TargetBasisPoints=CASE WHEN Tier='high' THEN 10000 ELSE 0 END WHERE SiteId=:site")
            db.execute("EXEC scheduler.SetSchedule @SiteId=:site,@WindowsJson=?",json.dumps(windows))
            db.execute("UPDATE scheduler.Site SET SchedulingEnabled=1,NextJoinUtc=NULL,NextLeaveUtc=NULL,LastReconciledUtc=NULL WHERE SiteId=:site")
        return saved

    @retry_deadlocks
    def restore(self,saved):
        with self.transaction() as db:
            db.execute("UPDATE scheduler.Site SET SchedulingEnabled=0 WHERE SiteId=:site")
            current=db.one("SELECT ConfigVersion FROM scheduler.Site WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=:site")
            # Preserve edits made through the customer UI while the pilot ran.
            if current['ConfigVersion']==saved['site']['ConfigVersion']+1:
                for a in saved['accounts']:
                    db.execute("UPDATE scheduler.AccountPolicy SET StakeTier=?,IsEnabled=?,CredentialRef=? WHERE SiteId=:site AND BotAccountId=?",a['StakeTier'],a['IsEnabled'],a['CredentialRef'],a['BotAccountId'])
                for b in saved['bands']:
                    db.execute("UPDATE scheduler.StakeBand SET TargetBasisPoints=? WHERE SiteId=:site AND Tier=?",b['TargetBasisPoints'],b['Tier'])
                windows=[{k:r[k] for k in ('Weekday','StartMinute','EndMinute','TargetEntries')} for r in saved['windows']]
                db.execute("EXEC scheduler.SetSchedule @SiteId=:site,@WindowsJson=?",json.dumps(windows))
            db.execute("UPDATE scheduler.MachineAssignment SET IsReady=0,IsEnabled=0 WHERE MachineId=:machine AND WorkerEpoch=?",self.epoch)

    @retry_deadlocks
    def confirm_absent(self,account_id,table_id):
        """Close an uncertain entry after the poker server confirms no seat exists."""
        with self.transaction() as db:
            machine=db.one("SELECT IsReady FROM scheduler.MachineAssignment WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=:site AND MachineId=:machine")
            if not machine or machine['IsReady']:
                raise RuntimeError('Cannot reconcile while the machine is ready')
            entry=db.one("SELECT EntryId,SessionId FROM scheduler.Entry WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=:site AND BotAccountId=? AND TableId=? AND IsOpen=1 AND Status='uncertain'",account_id,str(table_id))
            if not entry:
                raise RuntimeError('Expected one open uncertain entry')
            db.execute("UPDATE scheduler.Command SET Status='succeeded',IsOpen=0,ResultCode='reconciled_absent',UpdatedUtc=SYSUTCDATETIME() WHERE EntryId=? AND IsOpen=1",entry['EntryId'])
            db.execute("UPDATE scheduler.Entry SET Status='closed',IsOpen=0,ClosedUtc=SYSUTCDATETIME(),LastConfirmedUtc=SYSUTCDATETIME() WHERE EntryId=?",entry['EntryId'])
            db.execute("DELETE scheduler.AccountLease WHERE SiteId=:site AND BotAccountId=?",account_id)
            return entry['EntryId']

    @retry_deadlocks
    def confirm_account_absent(self,account_id):
        """Close all uncertain entries after an account-wide server absence check."""
        with self.transaction() as db:
            machine=db.one("SELECT IsReady FROM scheduler.MachineAssignment WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=:site AND MachineId=:machine")
            if not machine or machine['IsReady']:
                raise RuntimeError('Cannot reconcile while the machine is ready')
            entries=db.rows("SELECT EntryId,SessionId,Status FROM scheduler.Entry WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=:site AND BotAccountId=? AND IsOpen=1",account_id)
            if not entries or any(e['Status']!='uncertain' for e in entries):
                raise RuntimeError('Expected only open uncertain entries')
            for entry in entries:
                db.execute("UPDATE scheduler.Command SET Status='succeeded',IsOpen=0,ResultCode='reconciled_absent',UpdatedUtc=SYSUTCDATETIME() WHERE EntryId=? AND IsOpen=1",entry['EntryId'])
                db.execute("UPDATE scheduler.Entry SET Status='closed',IsOpen=0,ClosedUtc=SYSUTCDATETIME(),LastConfirmedUtc=SYSUTCDATETIME() WHERE EntryId=?",entry['EntryId'])
            db.execute("DELETE scheduler.AccountLease WHERE SiteId=:site AND BotAccountId=?",account_id)
            return [e['EntryId'] for e in entries]

    @retry_deadlocks
    def authorize_join(self,version):
        with self.transaction() as db:
            row=db.one("SELECT s.SchedulingEnabled,s.ConfigVersion,m.WorkerEpoch,m.IsEnabled,m.IsReady,m.MaintenanceTaskId FROM scheduler.Site s JOIN scheduler.MachineAssignment m ON m.SiteId=s.SiteId WHERE s.SiteId=:site AND m.MachineId=:machine")
            allowed=db.one("SELECT SiteId FROM backoffice.MachineAllowedSite WHERE MachineId=:machine AND SiteId=:site")
            return bool(allowed and not row['MaintenanceTaskId'] and row['SchedulingEnabled'] and row['IsEnabled'] and row['IsReady'] and row['ConfigVersion']==version and str(row['WorkerEpoch']).lower()==str(self.epoch).lower())

    @retry_deadlocks
    def publish(self,tables,seats,entries,sessions,intervals,*,ready=True,next_join=None,next_leave=None,last_plan=None):
        with self.transaction() as db:
            owned=db.one("SELECT WorkerEpoch FROM scheduler.MachineAssignment WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=:site AND MachineId=:machine")
            if not owned or str(owned['WorkerEpoch']).lower()!=str(self.epoch).lower(): raise RuntimeError('Worker epoch lost')
            db.execute("UPDATE scheduler.MachineAssignment SET LastHeartbeatUtc=SYSUTCDATETIME(),IsReady=?,LastSequence=LastSequence+1 WHERE MachineId=:machine",ready)
            db.execute("UPDATE backoffice.MachineMetadata SET LastAgentUtc=SYSUTCDATETIME() WHERE MachineId=:machine")
            if last_plan:
                db.execute("UPDATE scheduler.Site SET NextJoinUtc=?,NextLeaveUtc=?,LastReconciledUtc=? WHERE SiteId=:site",utc(next_join),utc(next_leave),utc(last_plan))
            for s in sessions:
                if not db.one('SELECT SessionId FROM scheduler.AccountSession WHERE SessionId=?',s['id']):
                    db.execute("INSERT scheduler.AccountSession(SessionId,SiteId,BotAccountId,MachineId,WorkerEpoch,StartedUtc,PlannedEndUtc,ActualEndUtc,IsOpen,EndReason) VALUES(?,:site,?,:machine,?,?,?,?,?,?)",s['id'],s['account'],self.epoch,utc(s['start']),utc(s['end']),utc(s.get('closed')),not bool(s.get('closed')),s.get('reason'))
                elif s.get('closed'):
                    # Only ever close; the scheduler may already have closed it, and must never see it reopened.
                    db.execute('UPDATE scheduler.AccountSession SET ActualEndUtc=?,IsOpen=0,EndReason=COALESCE(EndReason,?) WHERE SessionId=? AND IsOpen=1',utc(s.get('closed')),s.get('reason'),s['id'])
            for e in entries:
                opened=e['status'] not in ('closed','failed')
                if not db.one('SELECT EntryId FROM scheduler.Entry WHERE EntryId=?',e['id']):
                    db.execute("INSERT scheduler.Entry(EntryId,SiteId,BotAccountId,MachineId,WorkerEpoch,TableId,Status,IsOpen,CreatedUtc) VALUES(?,:site,?,:machine,?,?,?,?,?)",e['id'],e['account'],self.epoch,e['table'],e['status'],opened,utc(e['created']))
                db.execute('UPDATE scheduler.Entry SET Status=?,IsOpen=?,BuyInAmount=?,SeatedUtc=?,ClosedUtc=?,LastConfirmedUtc=?,SessionId=? WHERE EntryId=?',e['status'],opened,e.get('buy_in'),utc(e.get('seated')),utc(e.get('closed')),utc(e.get('confirmed')),e.get('session'),e['id'])
                for cmd in e.get('commands',[]):
                    is_open=cmd['status'] in ('pending','claimed','uncertain')
                    if not db.one('SELECT CommandId FROM scheduler.Command WHERE CommandId=?',cmd['id']):
                        db.execute("INSERT scheduler.Command(CommandId,SiteId,EntryId,MachineId,WorkerEpoch,Kind,Status,IsOpen,ConfigVersion,PayloadJson,NotBeforeUtc,ExpiresUtc) VALUES(?,:site,?,:machine,?,?,?,?,?,'{}',?,?)",cmd['id'],e['id'],self.epoch,cmd['kind'],cmd['status'],is_open,cmd['version'],utc(cmd['due']),utc(cmd['due']+timedelta(minutes=3)))
                    db.execute('UPDATE scheduler.Command SET Status=?,IsOpen=?,ResultCode=?,ResultText=?,UpdatedUtc=SYSUTCDATETIME() WHERE CommandId=?',cmd['status'],is_open,cmd.get('result'),cmd.get('result_text'),cmd['id'])
            active_accounts={e['account'] for e in entries if e['status'] not in ('closed','failed')}
            for aid in active_accounts:
                if not db.one('SELECT BotAccountId FROM scheduler.AccountLease WHERE BotAccountId=?',aid):
                    db.execute("INSERT scheduler.AccountLease(BotAccountId,SiteId,MachineId,WorkerEpoch,LeaseToken,LeaseUntilUtc,LastConfirmedUtc) VALUES(?,:site,:machine,?,?,DATEADD(second,90,SYSUTCDATETIME()),SYSUTCDATETIME())",aid,self.epoch,str(uuid.uuid4()))
                db.execute('UPDATE scheduler.AccountLease SET LeaseUntilUtc=DATEADD(second,90,SYSUTCDATETIME()),LastConfirmedUtc=SYSUTCDATETIME(),RequiresReconciliation=? WHERE BotAccountId=? AND WorkerEpoch=?',any(e['account']==aid and e['status']=='uncertain' for e in entries),aid,self.epoch)
            # A cancelled first join has no session yet, but still owns a lease.
            # Retain leases for uncertain/open entries; release only this worker's
            # leases once the persisted journal confirms no entries remain.
            db.execute("DELETE l FROM scheduler.AccountLease l WHERE l.SiteId=:site AND l.WorkerEpoch=? AND NOT EXISTS (SELECT 1 FROM scheduler.Entry e WHERE e.BotAccountId=l.BotAccountId AND e.IsOpen=1)",self.epoch)
            from .rules import split_interval
            for span in intervals:
                if db.one('SELECT IntervalId FROM scheduler.AccountInterval WHERE IntervalId=?',span['id']): continue
                db.execute("INSERT scheduler.AccountInterval(IntervalId,SiteId,BotAccountId,MachineId,WorkerEpoch,SequenceNumber,StartUtc,EndUtc,EntryCount) VALUES(?,:site,?,:machine,?,?,?,?,?)",span['id'],span['account'],self.epoch,span['sequence'],utc(span['start']),utc(span['end']),span['entries'])
                for day,seconds in split_interval(span['start'],span['end'],'Europe/Budapest').items():
                    count=int(seconds)
                    if not count: continue
                    if db.one("SELECT LocalDate FROM scheduler.AccountDay WHERE SiteId=:site AND BotAccountId=? AND LocalDate=?",span['account'],day):
                        db.execute("UPDATE scheduler.AccountDay SET SessionSeconds=SessionSeconds+?,EntrySeconds=EntrySeconds+?,UpdatedUtc=SYSUTCDATETIME() WHERE SiteId=:site AND BotAccountId=? AND LocalDate=?",count,count*span['entries'],span['account'],day)
                    else:
                        db.execute("INSERT scheduler.AccountDay(SiteId,BotAccountId,LocalDate,SessionSeconds,EntrySeconds) VALUES(:site,?,?,?,?)",span['account'],day,count,count*span['entries'])
        if tables or seats:
            self.publish_lobby(tables,seats)

    def publish_lobby(self,tables,seats):
        """Write lobby tables and seats in one round trip, in a transaction of their own.

        Every PC rewrites the same lobby rows each refresh. Writing them row by row inside the
        journal transaction kept those shared rows locked for seconds per PC, and the PCs
        queued behind each other for minutes. Heartbeats waited in that queue, so the
        scheduler saw every machine as stale and planned no joins, and the backoffice counted
        no confirmed seats until the queue drained.
        """
        latest={str(t['id']):t for t in tables}
        rows=[dict(id=tid,name=t['name'],game=t['game'],type=t.get('table_type','RING'),bb=t['bb'],min=t['minimum'],max=t['maximum'],
                   seats=t['seats'],occupied=t['occupied'],open=bool(t['open']),observed=iso(t['observed']),snapshot=iso(t.get('snapshot')))
              for tid,t in sorted(latest.items())]
        players=[dict(table=str(tid),seat=p['seat'],native=None if p['native_id'] is None else str(p['native_id']),name=p['name'],account=p['account'],
                      stack=p['stack'],bet=p['bet'],in_game=bool(p['in_game']),away=bool(p['away']),hand=None if p['hand'] is None else str(p['hand']),
                      observed=iso(p['observed']))
                 for tid,ps in seats.items() for p in ps]
        with self.transaction() as db:
            owned=db.one("SELECT WorkerEpoch FROM scheduler.MachineAssignment WHERE SiteId=:site AND MachineId=:machine")
            if not owned or str(owned['WorkerEpoch']).lower()!=str(self.epoch).lower(): raise RuntimeError('Worker epoch lost')
            db.execute(LOBBY_SQL,self.epoch,json.dumps(rows,default=str),json.dumps(sorted(str(t) for t in seats)),json.dumps(players,default=str))

    @retry_deadlocks
    def live_view_requests(self):
        """Return only unexpired requests assigned to this worker's machine."""
        with self.transaction() as db:
            db.execute("UPDATE scheduler.LiveViewRequest SET Status='expired' WHERE MachineId=:machine AND Status IN ('requested','active') AND ExpiresUtc<=SYSUTCDATETIME()")
            return db.rows("""SELECT r.RequestId,r.TableId,r.BotAccountId
                FROM scheduler.LiveViewRequest r
                JOIN scheduler.Entry e ON e.SiteId=r.SiteId AND e.TableId=r.TableId
                    AND e.BotAccountId=r.BotAccountId AND e.MachineId=r.MachineId AND e.IsOpen=1
                WHERE r.SiteId=:site AND r.MachineId=:machine AND r.Status IN ('requested','active')
                    AND r.ExpiresUtc>SYSUTCDATETIME()""")

    @retry_deadlocks
    def publish_live_view(self,request_id,hand_id,payload):
        with self.transaction() as db:
            request=db.one("""SELECT RequestId FROM scheduler.LiveViewRequest WITH(UPDLOCK,HOLDLOCK)
                WHERE RequestId=? AND SiteId=:site AND MachineId=:machine AND Status IN ('requested','active')
                    AND ExpiresUtc>SYSUTCDATETIME()""",request_id)
            if not request:
                return False
            if db.one('SELECT RequestId FROM scheduler.LiveTableSnapshot WHERE RequestId=?',request_id):
                db.execute("""UPDATE scheduler.LiveTableSnapshot SET SequenceNumber=SequenceNumber+1,
                    CapturedUtc=SYSUTCDATETIME(),HandId=?,PayloadJson=? WHERE RequestId=?""",str(hand_id or ''),json.dumps(payload),request_id)
            else:
                db.execute("INSERT scheduler.LiveTableSnapshot(RequestId,SequenceNumber,CapturedUtc,HandId,PayloadJson) VALUES(?,1,SYSUTCDATETIME(),?,?)",request_id,str(hand_id or ''),json.dumps(payload))
            db.execute("UPDATE scheduler.LiveViewRequest SET Status='active',LastSnapshotUtc=SYSUTCDATETIME(),ErrorText=NULL WHERE RequestId=?",request_id)
            return True
