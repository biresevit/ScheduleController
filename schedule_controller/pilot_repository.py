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
    MinBuyIn decimal(19,4),MaxBuyIn decimal(19,4),MaxSeats smallint,OccupiedSeats smallint,IsOpen bit,ObservedUtc datetime2(3),FullSnapshotUtc datetime2(3),
    StartUtc datetime2(3),TournamentState varchar(12));
INSERT @t SELECT * FROM OPENJSON(@tables) WITH(TableId nvarchar(80) '$.id',TableName nvarchar(200) '$.name',GameType varchar(8) '$.game',
    TableType varchar(4) '$.type',BigBlind decimal(19,4) '$.bb',MinBuyIn decimal(19,4) '$.min',MaxBuyIn decimal(19,4) '$.max',MaxSeats smallint '$.seats',
    OccupiedSeats smallint '$.occupied',IsOpen bit '$.open',ObservedUtc datetime2(3) '$.observed',FullSnapshotUtc datetime2(3) '$.snapshot',
    StartUtc datetime2(3) '$.start',TournamentState varchar(12) '$.state');
UPDATE l SET TableName=t.TableName,GameType=t.GameType,TableType=t.TableType,BigBlind=t.BigBlind,MinBuyIn=t.MinBuyIn,MaxBuyIn=t.MaxBuyIn,
    MaxSeats=t.MaxSeats,OccupiedSeats=t.OccupiedSeats,IsOpen=t.IsOpen,ObservedUtc=t.ObservedUtc,FullSnapshotUtc=t.FullSnapshotUtc,
    StartUtc=t.StartUtc,TournamentState=t.TournamentState,CurrencyCode=(SELECT CurrencyCode FROM scheduler.Site WHERE SiteId=@site),
    ObserverEpoch=@epoch,ObserverMachineId=@machine,SnapshotSequence=l.SnapshotSequence+1
    FROM scheduler.LobbyTable l JOIN @t t ON l.SiteId=@site AND l.TableId=t.TableId;
INSERT scheduler.LobbyTable(SiteId,TableId,TableName,GameType,TableType,BigBlind,MinBuyIn,MaxBuyIn,MaxSeats,OccupiedSeats,IsOpen,
    ObservedUtc,FullSnapshotUtc,ObserverEpoch,CurrencyCode,ObserverMachineId,SnapshotSequence,StartUtc,TournamentState)
    SELECT @site,t.TableId,t.TableName,t.GameType,t.TableType,t.BigBlind,t.MinBuyIn,t.MaxBuyIn,t.MaxSeats,t.OccupiedSeats,t.IsOpen,
        t.ObservedUtc,t.FullSnapshotUtc,@epoch,(SELECT CurrencyCode FROM scheduler.Site WHERE SiteId=@site),@machine,1,t.StartUtc,t.TournamentState
    FROM @t t WHERE NOT EXISTS(SELECT 1 FROM scheduler.LobbyTable l WHERE l.SiteId=@site AND l.TableId=t.TableId);
DELETE s FROM scheduler.TableSeat s WHERE s.SiteId=@site AND s.TableId IN (SELECT CONVERT(nvarchar(80),value) FROM OPENJSON(@seatTables));
INSERT scheduler.TableSeat(SiteId,TableId,SeatIndex,NativePlayerId,Username,BotAccountId,Stack,CurrentBet,InGame,SittingOut,HandId,ObservedUtc)
    SELECT @site,TableId,SeatIndex,NativePlayerId,Username,BotAccountId,Stack,CurrentBet,InGame,SittingOut,HandId,ObservedUtc
    FROM OPENJSON(@seats) WITH(TableId nvarchar(80) '$.table',SeatIndex smallint '$.seat',NativePlayerId nvarchar(80) '$.native',
        Username nvarchar(100) '$.name',BotAccountId int '$.account',Stack decimal(19,4) '$.stack',CurrentBet decimal(19,4) '$.bet',
        InGame bit '$.in_game',SittingOut bit '$.away',HandId nvarchar(80) '$.hand',ObservedUtc datetime2(3) '$.observed');"""


# One batch for the whole journal, in the order and with the rules of the former row-by-row writes.
JOURNAL_SQL="""SET NOCOUNT ON;
DECLARE @site int=:site,@machine int=:machine,@epoch uniqueidentifier=?,@ready bit=?,@hasPlan bit=?,
    @nextJoin datetime2(3)=?,@nextLeave datetime2(3)=?,@lastPlan datetime2(3)=?,
    @sessions nvarchar(max)=?,@entries nvarchar(max)=?,@commands nvarchar(max)=?,@leases nvarchar(max)=?,
    @intervals nvarchar(max)=?,@days nvarchar(max)=?;
UPDATE scheduler.MachineAssignment SET LastHeartbeatUtc=SYSUTCDATETIME(),IsReady=@ready,LastSequence=LastSequence+1
    WHERE SiteId=@site AND MachineId=@machine;
UPDATE backoffice.MachineMetadata SET LastAgentUtc=SYSUTCDATETIME() WHERE MachineId=@machine;
IF @hasPlan=1
    UPDATE scheduler.Site SET NextJoinUtc=@nextJoin,NextLeaveUtc=@nextLeave,LastReconciledUtc=@lastPlan WHERE SiteId=@site;

DECLARE @s TABLE(SessionId uniqueidentifier PRIMARY KEY,BotAccountId int,StartedUtc datetime2(3),PlannedEndUtc datetime2(3),
    ActualEndUtc datetime2(3),EndReason nvarchar(80));
INSERT @s SELECT * FROM OPENJSON(@sessions) WITH(SessionId uniqueidentifier '$.id',BotAccountId int '$.account',
    StartedUtc datetime2(3) '$.start',PlannedEndUtc datetime2(3) '$.end',ActualEndUtc datetime2(3) '$.closed',EndReason nvarchar(80) '$.reason');
-- Only ever close an existing session; the scheduler may already have closed it, and must never see it reopened.
UPDATE x SET ActualEndUtc=s.ActualEndUtc,IsOpen=0,EndReason=COALESCE(x.EndReason,s.EndReason)
    FROM scheduler.AccountSession x JOIN @s s ON s.SessionId=x.SessionId WHERE x.IsOpen=1 AND s.ActualEndUtc IS NOT NULL;
INSERT scheduler.AccountSession(SessionId,SiteId,BotAccountId,MachineId,WorkerEpoch,StartedUtc,PlannedEndUtc,ActualEndUtc,IsOpen,EndReason)
    SELECT s.SessionId,@site,s.BotAccountId,@machine,@epoch,s.StartedUtc,s.PlannedEndUtc,s.ActualEndUtc,
        CASE WHEN s.ActualEndUtc IS NULL THEN 1 ELSE 0 END,s.EndReason
    FROM @s s WHERE NOT EXISTS(SELECT 1 FROM scheduler.AccountSession x WHERE x.SessionId=s.SessionId);

DECLARE @e TABLE(EntryId uniqueidentifier PRIMARY KEY,BotAccountId int,TableId nvarchar(80),Status varchar(12),IsOpen bit,
    BuyInAmount decimal(19,4),CreatedUtc datetime2(3),SeatedUtc datetime2(3),ClosedUtc datetime2(3),LastConfirmedUtc datetime2(3),
    SessionId uniqueidentifier,PlayTableId nvarchar(80));
INSERT @e SELECT * FROM OPENJSON(@entries) WITH(EntryId uniqueidentifier '$.id',BotAccountId int '$.account',TableId nvarchar(80) '$.table',
    Status varchar(12) '$.status',IsOpen bit '$.open',BuyInAmount decimal(19,4) '$.buy_in',CreatedUtc datetime2(3) '$.created',
    SeatedUtc datetime2(3) '$.seated',ClosedUtc datetime2(3) '$.closed',LastConfirmedUtc datetime2(3) '$.confirmed',
    SessionId uniqueidentifier '$.session',PlayTableId nvarchar(80) '$.play_table');
INSERT scheduler.Entry(EntryId,SiteId,BotAccountId,MachineId,WorkerEpoch,TableId,Status,IsOpen,CreatedUtc)
    SELECT e.EntryId,@site,e.BotAccountId,@machine,@epoch,e.TableId,e.Status,e.IsOpen,e.CreatedUtc
    FROM @e e WHERE NOT EXISTS(SELECT 1 FROM scheduler.Entry x WHERE x.EntryId=e.EntryId);
-- PlayTableId: the tournament table a registered bot currently plays (NULL for ring entries).
UPDATE x SET Status=e.Status,IsOpen=e.IsOpen,BuyInAmount=e.BuyInAmount,SeatedUtc=e.SeatedUtc,ClosedUtc=e.ClosedUtc,
    LastConfirmedUtc=e.LastConfirmedUtc,SessionId=e.SessionId,PlayTableId=e.PlayTableId
    FROM scheduler.Entry x JOIN @e e ON e.EntryId=x.EntryId;

DECLARE @c TABLE(CommandId uniqueidentifier PRIMARY KEY,EntryId uniqueidentifier,Kind varchar(5),Status varchar(10),IsOpen bit,
    ConfigVersion bigint,NotBeforeUtc datetime2(3),ExpiresUtc datetime2(3),ResultCode nvarchar(80),ResultText nvarchar(1000),IsNew bit);
INSERT @c SELECT c.*,CASE WHEN EXISTS(SELECT 1 FROM scheduler.Command x WHERE x.CommandId=c.CommandId) THEN 0 ELSE 1 END
    FROM OPENJSON(@commands) WITH(CommandId uniqueidentifier '$.id',EntryId uniqueidentifier '$.entry',Kind varchar(5) '$.kind',
    Status varchar(10) '$.status',IsOpen bit '$.open',ConfigVersion bigint '$.version',NotBeforeUtc datetime2(3) '$.due',
    ExpiresUtc datetime2(3) '$.expires',ResultCode nvarchar(80) '$.result',ResultText nvarchar(1000) '$.result_text') c;
UPDATE x SET Status=c.Status,IsOpen=c.IsOpen,ResultCode=c.ResultCode,ResultText=c.ResultText,UpdatedUtc=SYSUTCDATETIME()
    FROM scheduler.Command x JOIN @c c ON c.CommandId=x.CommandId WHERE c.IsNew=0;
-- A command the worker started itself (e.g. leaving a busted seat) is being carried out now; one the scheduler planned
-- for the same entry and kind but no worker has claimed yet is superseded. Inserting next to it broke the
-- one-open-command rule: the save failed on every publish and stopped the site (machine 5, 2026-11-09 01:10).
UPDATE x SET Status='cancelled',IsOpen=0,ResultCode='superseded',ResultText='The PC already carries out this task for the bot.',
    UpdatedUtc=SYSUTCDATETIME()
    FROM scheduler.Command x WHERE x.IsOpen=1 AND x.Status='pending'
        AND EXISTS(SELECT 1 FROM @c c WHERE c.IsNew=1 AND c.IsOpen=1 AND c.EntryId=x.EntryId AND c.Kind=x.Kind)
        AND NOT EXISTS(SELECT 1 FROM @c c WHERE c.CommandId=x.CommandId);
INSERT scheduler.Command(CommandId,SiteId,EntryId,MachineId,WorkerEpoch,Kind,Status,IsOpen,ConfigVersion,PayloadJson,NotBeforeUtc,
    ExpiresUtc,ResultCode,ResultText,UpdatedUtc)
    SELECT c.CommandId,@site,c.EntryId,@machine,@epoch,c.Kind,c.Status,c.IsOpen,c.ConfigVersion,'{}',c.NotBeforeUtc,c.ExpiresUtc,
        c.ResultCode,c.ResultText,SYSUTCDATETIME()
    FROM @c c WHERE c.IsNew=1;

DECLARE @l TABLE(BotAccountId int PRIMARY KEY,Uncertain bit);
INSERT @l SELECT * FROM OPENJSON(@leases) WITH(BotAccountId int '$.account',Uncertain bit '$.uncertain');
INSERT scheduler.AccountLease(BotAccountId,SiteId,MachineId,WorkerEpoch,LeaseToken,LeaseUntilUtc,LastConfirmedUtc)
    SELECT l.BotAccountId,@site,@machine,@epoch,NEWID(),DATEADD(second,90,SYSUTCDATETIME()),SYSUTCDATETIME()
    FROM @l l WHERE NOT EXISTS(SELECT 1 FROM scheduler.AccountLease x WHERE x.BotAccountId=l.BotAccountId);
UPDATE x SET LeaseUntilUtc=DATEADD(second,90,SYSUTCDATETIME()),LastConfirmedUtc=SYSUTCDATETIME(),RequiresReconciliation=l.Uncertain
    FROM scheduler.AccountLease x JOIN @l l ON l.BotAccountId=x.BotAccountId WHERE x.WorkerEpoch=@epoch;
-- A cancelled first join has no session yet, but still owns a lease. Retain leases for uncertain/open entries;
-- release only this worker's leases once the persisted journal confirms no entries remain.
DELETE l FROM scheduler.AccountLease l WHERE l.SiteId=@site AND l.WorkerEpoch=@epoch
    AND NOT EXISTS(SELECT 1 FROM scheduler.Entry e WHERE e.BotAccountId=l.BotAccountId AND e.IsOpen=1);

DECLARE @i TABLE(IntervalId uniqueidentifier PRIMARY KEY,BotAccountId int,SequenceNumber bigint,StartUtc datetime2(3),EndUtc datetime2(3),EntryCount int);
INSERT @i SELECT * FROM OPENJSON(@intervals) WITH(IntervalId uniqueidentifier '$.id',BotAccountId int '$.account',
    SequenceNumber bigint '$.sequence',StartUtc datetime2(3) '$.start',EndUtc datetime2(3) '$.end',EntryCount int '$.entries');
DECLARE @new TABLE(IntervalId uniqueidentifier PRIMARY KEY);
-- A retried publish must not account an interval twice.
INSERT scheduler.AccountInterval(IntervalId,SiteId,BotAccountId,MachineId,WorkerEpoch,SequenceNumber,StartUtc,EndUtc,EntryCount)
    OUTPUT inserted.IntervalId INTO @new
    SELECT i.IntervalId,@site,i.BotAccountId,@machine,@epoch,i.SequenceNumber,i.StartUtc,i.EndUtc,i.EntryCount
    FROM @i i WHERE NOT EXISTS(SELECT 1 FROM scheduler.AccountInterval x WHERE x.IntervalId=i.IntervalId);
DECLARE @day TABLE(BotAccountId int,LocalDate date,SessionSeconds bigint,EntrySeconds bigint,PRIMARY KEY(BotAccountId,LocalDate));
INSERT @day SELECT d.BotAccountId,d.LocalDate,SUM(d.Seconds),SUM(d.EntrySeconds)
    FROM OPENJSON(@days) WITH(IntervalId uniqueidentifier '$.interval',BotAccountId int '$.account',LocalDate date '$.day',
        Seconds bigint '$.seconds',EntrySeconds bigint '$.entry_seconds') d
    JOIN @new n ON n.IntervalId=d.IntervalId GROUP BY d.BotAccountId,d.LocalDate;
UPDATE x SET SessionSeconds=x.SessionSeconds+d.SessionSeconds,EntrySeconds=x.EntrySeconds+d.EntrySeconds,UpdatedUtc=SYSUTCDATETIME()
    FROM scheduler.AccountDay x JOIN @day d ON x.SiteId=@site AND x.BotAccountId=d.BotAccountId AND x.LocalDate=d.LocalDate;
INSERT scheduler.AccountDay(SiteId,BotAccountId,LocalDate,SessionSeconds,EntrySeconds)
    SELECT @site,d.BotAccountId,d.LocalDate,d.SessionSeconds,d.EntrySeconds FROM @day d
    WHERE NOT EXISTS(SELECT 1 FROM scheduler.AccountDay x WHERE x.SiteId=@site AND x.BotAccountId=d.BotAccountId AND x.LocalDate=d.LocalDate);"""


def stamp_ms(value):
    """ISO text at millisecond precision, the columns' precision, for OPENJSON."""
    value=utc(value)
    return value.isoformat(timespec='milliseconds') if value else None


def journal_payload(entries,sessions,intervals):
    """The JSON parameters of JOURNAL_SQL: sessions, entries, commands, leases, intervals and their local days."""
    from .rules import split_interval
    ms=lambda v:v.replace(microsecond=v.microsecond//1000*1000)
    session_rows={str(s['id']):dict(id=str(s['id']),account=s['account'],start=stamp_ms(s['start']),end=stamp_ms(s['end']),
                                     closed=stamp_ms(s['closed']) if s.get('closed') else None,reason=s.get('reason'))
                  for s in sessions}
    entry_rows={};command_rows={}
    for e in entries:
        opened=e['status'] not in ('closed','failed')
        entry_rows[str(e['id'])]=dict(id=str(e['id']),account=e['account'],table=str(e['table']),status=e['status'],open=opened,
            buy_in=e.get('buy_in'),created=stamp_ms(e['created']),seated=stamp_ms(e.get('seated')),closed=stamp_ms(e.get('closed')),
            confirmed=stamp_ms(e.get('confirmed')),session=None if e.get('session') is None else str(e['session']),
            play_table=None if e.get('play_table') is None else str(e['play_table']))
        for cmd in e.get('commands',[]):
            command_rows[str(cmd['id'])]=dict(id=str(cmd['id']),entry=str(e['id']),kind=cmd['kind'],status=cmd['status'],
                open=cmd['status'] in ('pending','claimed','uncertain'),version=cmd['version'],due=stamp_ms(cmd['due']),
                expires=stamp_ms(cmd['due']+timedelta(minutes=3)),result=cmd.get('result'),result_text=cmd.get('result_text'))
    active={e['account'] for e in entries if e['status'] not in ('closed','failed')}
    leases=[dict(account=a,uncertain=any(e['account']==a and e['status']=='uncertain' for e in entries)) for a in sorted(active)]
    interval_rows={};days=[]
    for span in intervals:
        # an empty interval at millisecond precision has nothing to account and would violate EndUtc>StartUtc
        if ms(span['end'])<=ms(span['start']) or str(span['id']) in interval_rows: continue
        interval_rows[str(span['id'])]=dict(id=str(span['id']),account=span['account'],sequence=span['sequence'],
            start=stamp_ms(span['start']),end=stamp_ms(span['end']),entries=span['entries'])
        for day,seconds in split_interval(span['start'],span['end'],'Europe/Budapest').items():
            count=int(seconds)
            if count:
                days.append(dict(interval=str(span['id']),account=span['account'],day=str(day),seconds=count,
                                 entry_seconds=count*span['entries']))
    dump=lambda rows:json.dumps(list(rows),default=str)
    return (dump(session_rows.values()),dump(entry_rows.values()),dump(command_rows.values()),dump(leases),
            dump(interval_rows.values()),dump(days))

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


@retry_deadlocks
def machine_sites(machine_id):
    """The poker sites the PC's worker runs: those the PC plays, and a site taken off it until its tables are left."""
    import pyodbc
    conn=pyodbc.connect(connection_string(),timeout=15)
    try:
        rows=conn.cursor().execute("""SELECT a.SiteId FROM scheduler.MachineAssignment a
            JOIN dbo.CustomerMachine m ON m.MachineId=a.MachineId AND m.IsActive=1
            WHERE a.MachineId=? AND (EXISTS(SELECT 1 FROM backoffice.MachineAllowedSite s WHERE s.MachineId=a.MachineId AND s.SiteId=a.SiteId)
                OR EXISTS(SELECT 1 FROM scheduler.Entry e WHERE e.SiteId=a.SiteId AND e.MachineId=a.MachineId AND e.IsOpen=1))
            ORDER BY a.SiteId""",machine_id).fetchall()
        return [r[0] for r in rows]
    finally: conn.close()


class Repository(ChatRepository,ManualRepository):
    def __init__(self,epoch,site_id=1,machine_id=1):
        self.epoch=epoch;self.site_id=int(site_id);self.machine_id=int(machine_id)

    @retry_deadlocks
    def join_session_block(self, session_id):
        """Called under the login's execution lock before every server-side join."""
        if not session_id:
            return 'session_ended'
        with self.transaction() as db:
            row=db.one("""SELECT s.IsOpen,s.EndReason,s.PlannedEndUtc,
                    CASE WHEN s.PlannedEndUtc>SYSUTCDATETIME() THEN 1 ELSE 0 END AS NotEnded,
                    (SELECT COUNT(*) FROM scheduler.Command c JOIN scheduler.Entry e ON e.EntryId=c.EntryId
                     WHERE e.SessionId=s.SessionId AND c.Kind='join' AND c.Status='failed') AS Failures
                FROM scheduler.AccountSession s
                JOIN scheduler.MachineAssignment a ON a.SiteId=s.SiteId AND a.MachineId=s.MachineId
                WHERE s.SiteId=:site AND s.MachineId=:machine
                  AND a.WorkerEpoch=? AND s.SessionId=?""",self.epoch,session_id)
            if row and (row['EndReason']=='join_failure_limit' or row['Failures']>=2):
                return 'join_failure_limit'
            return None if row and row['IsOpen'] and row['NotEnded'] else 'session_ended'

    @retry_deadlocks
    def enforce_join_failure_limit(self):
        from .join_failures import limit_failed_sessions
        with self.transaction() as db:
            limit_failed_sessions(db,self.site_id)
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
    def machine_enabled(self):
        """False when the PC is switched off in the backoffice (or this site is taken off it)."""
        with self.transaction() as db:
            row=db.one("SELECT IsEnabled FROM scheduler.MachineAssignment WHERE SiteId=:site AND MachineId=:machine")
            return bool(row and row['IsEnabled'])

    @retry_deadlocks
    def release_observer(self,account):
        with self.transaction() as db:
            db.execute("""DELETE l FROM scheduler.AccountLease l WHERE l.SiteId=:site AND l.MachineId=:machine AND l.WorkerEpoch=? AND l.BotAccountId=?
                AND NOT EXISTS(SELECT 1 FROM scheduler.Entry e WHERE e.BotAccountId=l.BotAccountId AND e.IsOpen=1)""",self.epoch,account)

    @retry_deadlocks
    def configuration(self):
        with self.transaction() as db:
            site=db.one("SELECT s.* FROM scheduler.Site s JOIN scheduler.MachineAssignment a ON a.SiteId=s.SiteId JOIN dbo.CustomerMachine m ON m.MachineId=a.MachineId AND m.CustomerId=s.CustomerId JOIN dbo.MachineCasino mc ON mc.MachineId=a.MachineId AND mc.Casino=s.Casino WHERE s.SiteId=:site AND a.MachineId=:machine AND m.IsActive=1")
            if not site: raise RuntimeError('Registered machine assignment for this site not found')
            accounts=db.rows("SELECT p.*,b.Username,b.IsActive,b.LoginEmail,b.LoginPassword FROM scheduler.AccountPolicy p JOIN dbo.BotAccount b ON b.BotAccountId=p.BotAccountId WHERE p.SiteId=:site AND p.IsSynthetic=0 AND p.DeletedUtc IS NULL")
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
    def start_cloud_worker(self,previous_worker_gone=False):
        """Fence the old epoch and return every entry that needs live reconciliation.

        previous_worker_gone: this PC holds the worker lock of its machine and site (WorkerLock), so the worker
        behind a still-fresh heartbeat has exited (e.g. crashed) and need not be waited out."""
        with self.transaction() as db:
            machine=db.one("SELECT * FROM scheduler.MachineAssignment WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=:site AND MachineId=:machine")
            if not machine: raise RuntimeError('Machine assignment not found')
            cutoff=datetime.now(timezone.utc).replace(tzinfo=None)-timedelta(seconds=90)
            if not previous_worker_gone and machine['LastHeartbeatUtc'] and machine['IsReady'] and machine['LastHeartbeatUtc']>cutoff:
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
        # 'joining': a tournament registration still waiting for its tournament to start.
        if status not in ('seated','sitting_out','joining'):
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
            rows=db.rows("""SELECT TOP (?) c.CommandId,c.EntryId,c.Kind,c.ConfigVersion,c.NotBeforeUtc,c.PayloadJson,
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
            db.execute("UPDATE scheduler.MachineAssignment SET IsReady=0,IsEnabled=0 WHERE SiteId=:site AND MachineId=:machine AND WorkerEpoch=?",self.epoch)

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
        """Lobby first (entries reference its tables), then the journal in one set-based batch.

        The journal used to be written row by row: about 800 round trips at 90 entries (machine 5, 2026-11-09),
        25-40 s per publish from a PC far from the database. The lobby rows, stamped at scan time, then reached
        the database already stale and the heartbeat looked as old, so the scheduler stopped planning joins.
        """
        if tables or seats:
            self.publish_lobby(tables,seats)
        payload=journal_payload(entries,sessions,intervals)
        with self.transaction() as db:
            owned=db.one("SELECT WorkerEpoch FROM scheduler.MachineAssignment WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=:site AND MachineId=:machine")
            if not owned or str(owned['WorkerEpoch']).lower()!=str(self.epoch).lower(): raise RuntimeError('Worker epoch lost')
            db.execute(JOURNAL_SQL,self.epoch,bool(ready),bool(last_plan),utc(next_join),utc(next_leave),utc(last_plan),*payload)

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
                   seats=t['seats'],occupied=t['occupied'],open=bool(t['open']),observed=iso(t['observed']),snapshot=iso(t.get('snapshot')),
                   start=iso(t.get('start')),state=t.get('tournament_state'))
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
            return db.rows("""SELECT r.RequestId,r.TableId,r.BotAccountId,JSON_VALUE(s.PayloadJson,'$.table_id') AS BoundTableId
                FROM scheduler.LiveViewRequest r
                LEFT JOIN scheduler.LiveTableSnapshot s ON s.RequestId=r.RequestId
                JOIN scheduler.Entry e ON e.SiteId=r.SiteId AND e.TableId=r.TableId
                    AND e.BotAccountId=r.BotAccountId AND e.MachineId=r.MachineId AND e.IsOpen=1
                WHERE r.SiteId=:site AND r.MachineId=:machine AND r.Status IN ('requested','active')
                    AND r.ExpiresUtc>SYSUTCDATETIME()""")

    @retry_deadlocks
    def publish_live_view(self,request_id,hand_id,payload):
        with self.transaction() as db:
            # Match the website's physical-table lock before touching the request row.
            # This also serializes first publication with a concurrent manual switch.
            aid=db.one("""SELECT r.BotAccountId,JSON_VALUE(s.PayloadJson,'$.table_id') AS BoundTableId
                FROM scheduler.LiveViewRequest r LEFT JOIN scheduler.LiveTableSnapshot s ON s.RequestId=r.RequestId
                WHERE r.RequestId=? AND r.SiteId=:site AND r.MachineId=:machine""",request_id)
            if not aid:return False
            self._manual_lock(db,aid['BotAccountId'],aid.get('BoundTableId') or payload['table_id'])
            request=db.one("""SELECT RequestId,TableId,BotAccountId FROM scheduler.LiveViewRequest WITH(UPDLOCK,HOLDLOCK)
                WHERE RequestId=? AND SiteId=:site AND MachineId=:machine AND Status IN ('requested','active')
                    AND ExpiresUtc>SYSUTCDATETIME()""",request_id)
            if not request:
                return False
            snapshot=db.one("SELECT RequestId,JSON_VALUE(PayloadJson,'$.table_id') AS BoundTableId FROM scheduler.LiveTableSnapshot WHERE RequestId=?",request_id)
            if snapshot and str(request['TableId']).startswith('T') and str(snapshot['BoundTableId'])!=str(payload['table_id']):
                self._manual_lock(db,request['BotAccountId'],snapshot['BoundTableId'])
                self._end_live_table_views(db,request['BotAccountId'],snapshot['BoundTableId'])
                return False
            if snapshot:
                db.execute("""UPDATE scheduler.LiveTableSnapshot SET SequenceNumber=SequenceNumber+1,
                    CapturedUtc=SYSUTCDATETIME(),HandId=?,PayloadJson=? WHERE RequestId=?""",str(hand_id or ''),json.dumps(payload),request_id)
            else:
                db.execute("INSERT scheduler.LiveTableSnapshot(RequestId,SequenceNumber,CapturedUtc,HandId,PayloadJson) VALUES(?,1,SYSUTCDATETIME(),?,?)",request_id,str(hand_id or ''),json.dumps(payload))
            db.execute("UPDATE scheduler.LiveViewRequest SET Status='active',LastSnapshotUtc=SYSUTCDATETIME(),ErrorText=NULL WHERE RequestId=?",request_id)
            return True
