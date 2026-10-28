"""SQL journal and telemetry for the explicitly bounded single-PC demo pilot.

This is not the distributed queue consumer. It refuses existing open entries and
uses a machine epoch so an old process cannot overwrite a replacement worker.
"""
from contextlib import contextmanager
from datetime import datetime,timedelta,timezone
import json
import socket
import uuid

from .database import connection_string

NAMES=("richard","kate","john","jeremy","mason","monica","emma","andrew")


def utc(value):
    return value.replace(tzinfo=None) if value else None


class DB:
    def __init__(self,c): self.c=c
    def rows(self,sql,*args):
        self.c.execute(sql,*args)
        names=[x[0] for x in self.c.description]
        return [dict(zip(names,row)) for row in self.c.fetchall()]
    def one(self,sql,*args):
        return next(iter(self.rows(sql,*args)),None)
    def execute(self,sql,*args): self.c.execute(sql,*args)


class Repository:
    def __init__(self,epoch): self.epoch=epoch
    @contextmanager
    def transaction(self):
        import pyodbc
        conn=pyodbc.connect(connection_string(),autocommit=False,timeout=15)
        try:
            db=DB(conn.cursor())
            db.execute('SET XACT_ABORT ON')
            yield db
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally: conn.close()

    def configuration(self):
        with self.transaction() as db:
            site=db.one("SELECT * FROM scheduler.Site WHERE SiteId=1 AND CustomerId=14004 AND Casino='Evenbet'")
            if not site: raise RuntimeError('Nenad/Evenbet site not found')
            accounts=db.rows("SELECT p.*,b.Username,b.IsActive FROM scheduler.AccountPolicy p JOIN dbo.BotAccount b ON b.BotAccountId=p.BotAccountId WHERE p.SiteId=1 AND p.IsSynthetic=0 AND p.DeletedUtc IS NULL")
            accounts=[a for a in accounts if a['Username'] in NAMES]
            if len(accounts)!=8: raise RuntimeError('Expected exactly the eight authorized demo accounts')
            from zoneinfo import ZoneInfo
            today=datetime.now(ZoneInfo(site['TimeZone'])).date()
            for a in accounts:
                row=db.one('SELECT SessionSeconds FROM scheduler.AccountDay WHERE SiteId=1 AND BotAccountId=? AND LocalDate=?',a['BotAccountId'],today)
                a['PlayedSeconds']=row['SessionSeconds'] if row else 0
            return {'site':site,'accounts':accounts,'bands':db.rows('SELECT * FROM scheduler.StakeBand WHERE SiteId=1'),
                'windows':db.rows('SELECT * FROM scheduler.ScheduleWindow WHERE SiteId=1'),
                'machine':db.one('SELECT * FROM scheduler.MachineAssignment WHERE SiteId=1 AND MachineId=1')}

    def start(self,capacity):
        with self.transaction() as db:
            machine=db.one('SELECT * FROM scheduler.MachineAssignment WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=1 AND MachineId=1')
            if not machine: raise RuntimeError('Machine assignment not found')
            if machine['LastHeartbeatUtc'] and machine['IsReady'] and machine['LastHeartbeatUtc']>datetime.now(timezone.utc).replace(tzinfo=None)-timedelta(seconds=90):
                raise RuntimeError('Machine has a live worker')
            if db.one('SELECT TOP(1) EntryId FROM scheduler.Entry WHERE SiteId=1 AND IsOpen=1'):
                raise RuntimeError('Open entries require reconciliation before the pilot')
            db.execute("UPDATE scheduler.MachineAssignment SET MaxEntries=?,IsReady=1,IsEnabled=1,WorkerEpoch=?,LastHeartbeatUtc=SYSUTCDATETIME(),ControllerVersion='local-pilot-0.1',DecisionUrl='http://127.0.0.1:5005' WHERE MachineId=1 AND SiteId=1",capacity,self.epoch)
            db.execute('UPDATE backoffice.MachineMetadata SET HostName=?,LastAgentUtc=SYSUTCDATETIME(),Version=Version+1 WHERE MachineId=1',socket.gethostname())

    def start_cloud_worker(self):
        """Register this process as the sole command worker for Machine 1.

        Recovery of seats owned by another epoch is intentionally not guessed.
        A replacement worker starts only from a clean journal until explicit
        reconciliation support is added.
        """
        with self.transaction() as db:
            machine=db.one('SELECT * FROM scheduler.MachineAssignment WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=1 AND MachineId=1')
            if not machine: raise RuntimeError('Machine assignment not found')
            cutoff=datetime.now(timezone.utc).replace(tzinfo=None)-timedelta(seconds=90)
            if machine['LastHeartbeatUtc'] and machine['IsReady'] and machine['LastHeartbeatUtc']>cutoff:
                raise RuntimeError('Machine has a live worker')
            if db.one('SELECT TOP(1) EntryId FROM scheduler.Entry WHERE SiteId=1 AND IsOpen=1'):
                raise RuntimeError('Open entries require reconciliation before starting the cloud worker')
            db.execute("""UPDATE scheduler.MachineAssignment SET IsReady=1,IsEnabled=1,
                WorkerEpoch=?,LastHeartbeatUtc=SYSUTCDATETIME(),ControllerVersion='poker-controller-cloud-worker-0.1',
                DecisionUrl='http://127.0.0.1:5005' WHERE MachineId=1 AND SiteId=1""",self.epoch)
            db.execute('UPDATE backoffice.MachineMetadata SET HostName=?,LastAgentUtc=SYSUTCDATETIME(),Version=Version+1 WHERE MachineId=1',socket.gethostname())

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
                WHERE c.SiteId=1 AND c.MachineId=1 AND c.WorkerEpoch=? AND c.IsOpen=1
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

    def stop_cloud_worker(self):
        with self.transaction() as db:
            db.execute("""UPDATE scheduler.MachineAssignment SET IsReady=0,LastHeartbeatUtc=SYSUTCDATETIME()
                WHERE SiteId=1 AND MachineId=1 AND WorkerEpoch=?""",self.epoch)

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
                db.execute('UPDATE scheduler.AccountPolicy SET IsEnabled=1,CredentialRef=? WHERE SiteId=1 AND BotAccountId=?','local-demo:'+a['Username'],a['BotAccountId'])
            if high_only:
                for a in saved['accounts']:
                    db.execute("UPDATE scheduler.AccountPolicy SET StakeTier='high' WHERE BotAccountId=? AND SiteId=1",a['BotAccountId'])
                db.execute("UPDATE scheduler.StakeBand SET TargetBasisPoints=CASE WHEN Tier='high' THEN 10000 ELSE 0 END WHERE SiteId=1")
            db.execute('EXEC scheduler.SetSchedule @SiteId=1,@WindowsJson=?',json.dumps(windows))
            db.execute('UPDATE scheduler.Site SET SchedulingEnabled=1,NextJoinUtc=NULL,NextLeaveUtc=NULL,LastReconciledUtc=NULL WHERE SiteId=1')
        return saved

    def restore(self,saved):
        with self.transaction() as db:
            db.execute('UPDATE scheduler.Site SET SchedulingEnabled=0 WHERE SiteId=1')
            current=db.one('SELECT ConfigVersion FROM scheduler.Site WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=1')
            # Preserve edits made through the customer UI while the pilot ran.
            if current['ConfigVersion']==saved['site']['ConfigVersion']+1:
                for a in saved['accounts']:
                    db.execute('UPDATE scheduler.AccountPolicy SET StakeTier=?,IsEnabled=?,CredentialRef=? WHERE SiteId=1 AND BotAccountId=?',a['StakeTier'],a['IsEnabled'],a['CredentialRef'],a['BotAccountId'])
                for b in saved['bands']:
                    db.execute('UPDATE scheduler.StakeBand SET TargetBasisPoints=? WHERE SiteId=1 AND Tier=?',b['TargetBasisPoints'],b['Tier'])
                windows=[{k:r[k] for k in ('Weekday','StartMinute','EndMinute','TargetEntries')} for r in saved['windows']]
                db.execute('EXEC scheduler.SetSchedule @SiteId=1,@WindowsJson=?',json.dumps(windows))
            db.execute('UPDATE scheduler.MachineAssignment SET IsReady=0,IsEnabled=0 WHERE MachineId=1 AND WorkerEpoch=?',self.epoch)

    def confirm_absent(self,account_id,table_id):
        """Close an uncertain entry after the poker server confirms no seat exists."""
        with self.transaction() as db:
            machine=db.one('SELECT IsReady FROM scheduler.MachineAssignment WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=1 AND MachineId=1')
            if not machine or machine['IsReady']:
                raise RuntimeError('Cannot reconcile while the machine is ready')
            entry=db.one("SELECT EntryId,SessionId FROM scheduler.Entry WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=1 AND BotAccountId=? AND TableId=? AND IsOpen=1 AND Status='uncertain'",account_id,str(table_id))
            if not entry:
                raise RuntimeError('Expected one open uncertain entry')
            db.execute("UPDATE scheduler.Command SET Status='succeeded',IsOpen=0,ResultCode='reconciled_absent',UpdatedUtc=SYSUTCDATETIME() WHERE EntryId=? AND IsOpen=1",entry['EntryId'])
            db.execute("UPDATE scheduler.Entry SET Status='closed',IsOpen=0,ClosedUtc=SYSUTCDATETIME(),LastConfirmedUtc=SYSUTCDATETIME() WHERE EntryId=?",entry['EntryId'])
            if entry['SessionId']:
                db.execute("UPDATE scheduler.AccountSession SET ActualEndUtc=SYSUTCDATETIME(),IsOpen=0,EndReason='reconciled_absent' WHERE SessionId=? AND IsOpen=1",entry['SessionId'])
            db.execute('DELETE scheduler.AccountLease WHERE SiteId=1 AND BotAccountId=?',account_id)
            return entry['EntryId']

    def confirm_account_absent(self,account_id):
        """Close all uncertain entries after an account-wide server absence check."""
        with self.transaction() as db:
            machine=db.one('SELECT IsReady FROM scheduler.MachineAssignment WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=1 AND MachineId=1')
            if not machine or machine['IsReady']:
                raise RuntimeError('Cannot reconcile while the machine is ready')
            entries=db.rows("SELECT EntryId,SessionId,Status FROM scheduler.Entry WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=1 AND BotAccountId=? AND IsOpen=1",account_id)
            if not entries or any(e['Status']!='uncertain' for e in entries):
                raise RuntimeError('Expected only open uncertain entries')
            for entry in entries:
                db.execute("UPDATE scheduler.Command SET Status='succeeded',IsOpen=0,ResultCode='reconciled_absent',UpdatedUtc=SYSUTCDATETIME() WHERE EntryId=? AND IsOpen=1",entry['EntryId'])
                db.execute("UPDATE scheduler.Entry SET Status='closed',IsOpen=0,ClosedUtc=SYSUTCDATETIME(),LastConfirmedUtc=SYSUTCDATETIME() WHERE EntryId=?",entry['EntryId'])
            session_ids={e['SessionId'] for e in entries if e['SessionId']}
            for session_id in session_ids:
                db.execute("UPDATE scheduler.AccountSession SET ActualEndUtc=SYSUTCDATETIME(),IsOpen=0,EndReason='reconciled_absent' WHERE SessionId=? AND IsOpen=1",session_id)
            db.execute('DELETE scheduler.AccountLease WHERE SiteId=1 AND BotAccountId=?',account_id)
            return [e['EntryId'] for e in entries]

    def authorize_join(self,version):
        with self.transaction() as db:
            row=db.one('SELECT s.SchedulingEnabled,s.ConfigVersion,m.WorkerEpoch,m.IsEnabled,m.IsReady FROM scheduler.Site s JOIN scheduler.MachineAssignment m ON m.SiteId=s.SiteId WHERE s.SiteId=1 AND m.MachineId=1')
            allowed=db.one('SELECT SiteId FROM backoffice.MachineAllowedSite WHERE MachineId=1 AND SiteId=1')
            return bool(allowed and row['SchedulingEnabled'] and row['IsEnabled'] and row['IsReady'] and row['ConfigVersion']==version and str(row['WorkerEpoch']).lower()==str(self.epoch).lower())

    def publish(self,tables,seats,entries,sessions,intervals,*,ready=True,next_join=None,next_leave=None,last_plan=None):
        with self.transaction() as db:
            owned=db.one('SELECT WorkerEpoch FROM scheduler.MachineAssignment WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=1 AND MachineId=1')
            if not owned or str(owned['WorkerEpoch']).lower()!=str(self.epoch).lower(): raise RuntimeError('Worker epoch lost')
            db.execute('UPDATE scheduler.MachineAssignment SET LastHeartbeatUtc=SYSUTCDATETIME(),IsReady=?,LastSequence=LastSequence+1 WHERE MachineId=1',ready)
            db.execute('UPDATE backoffice.MachineMetadata SET LastAgentUtc=SYSUTCDATETIME() WHERE MachineId=1')
            if last_plan:
                db.execute('UPDATE scheduler.Site SET NextJoinUtc=?,NextLeaveUtc=?,LastReconciledUtc=? WHERE SiteId=1',utc(next_join),utc(next_leave),utc(last_plan))
            for t in tables:
                values=(t['name'],t['game'],t.get('table_type','RING'),t['bb'],t['minimum'],t['maximum'],t['seats'],t['occupied'],t['open'],utc(t['observed']),utc(t.get('snapshot')),self.epoch,t['id'])
                if db.one('SELECT TableId FROM scheduler.LobbyTable WHERE SiteId=1 AND TableId=?',t['id']):
                    db.execute('UPDATE scheduler.LobbyTable SET TableName=?,GameType=?,TableType=?,BigBlind=?,MinBuyIn=?,MaxBuyIn=?,MaxSeats=?,OccupiedSeats=?,IsOpen=?,ObservedUtc=?,FullSnapshotUtc=?,ObserverEpoch=?,SnapshotSequence=SnapshotSequence+1 WHERE SiteId=1 AND TableId=?',*values)
                else:
                    db.execute("INSERT scheduler.LobbyTable(SiteId,TableName,GameType,TableType,BigBlind,MinBuyIn,MaxBuyIn,MaxSeats,OccupiedSeats,IsOpen,ObservedUtc,FullSnapshotUtc,ObserverEpoch,TableId,CurrencyCode,ObserverMachineId,SnapshotSequence) VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,'DEMO',1,1)",*values)
            for tid,players in seats.items():
                db.execute('DELETE scheduler.TableSeat WHERE SiteId=1 AND TableId=?',tid)
                for p in players:
                    db.execute('INSERT scheduler.TableSeat(SiteId,TableId,SeatIndex,NativePlayerId,Username,BotAccountId,Stack,CurrentBet,InGame,SittingOut,HandId,ObservedUtc) VALUES(1,?,?,?,?,?,?,?,?,?,?,?)',tid,p['seat'],p['native_id'],p['name'],p['account'],p['stack'],p['bet'],p['in_game'],p['away'],p['hand'],utc(p['observed']))
            for s in sessions:
                if not db.one('SELECT SessionId FROM scheduler.AccountSession WHERE SessionId=?',s['id']):
                    db.execute('INSERT scheduler.AccountSession(SessionId,SiteId,BotAccountId,MachineId,WorkerEpoch,StartedUtc,PlannedEndUtc,ActualEndUtc,IsOpen,EndReason) VALUES(?,1,?,1,?,?,?,?,?,?)',s['id'],s['account'],self.epoch,utc(s['start']),utc(s['end']),utc(s.get('closed')),not bool(s.get('closed')),s.get('reason'))
                else:
                    db.execute('UPDATE scheduler.AccountSession SET ActualEndUtc=?,IsOpen=?,EndReason=? WHERE SessionId=?',utc(s.get('closed')),not bool(s.get('closed')),s.get('reason'),s['id'])
            for e in entries:
                opened=e['status'] not in ('closed','failed')
                if not db.one('SELECT EntryId FROM scheduler.Entry WHERE EntryId=?',e['id']):
                    db.execute('INSERT scheduler.Entry(EntryId,SiteId,BotAccountId,MachineId,WorkerEpoch,TableId,Status,IsOpen,CreatedUtc) VALUES(?,1,?,1,?,?,?,?,?)',e['id'],e['account'],self.epoch,e['table'],e['status'],opened,utc(e['created']))
                db.execute('UPDATE scheduler.Entry SET Status=?,IsOpen=?,BuyInAmount=?,SeatedUtc=?,ClosedUtc=?,LastConfirmedUtc=?,SessionId=? WHERE EntryId=?',e['status'],opened,e.get('buy_in'),utc(e.get('seated')),utc(e.get('closed')),utc(e.get('confirmed')),e.get('session'),e['id'])
                for cmd in e.get('commands',[]):
                    is_open=cmd['status'] in ('pending','claimed','uncertain')
                    if not db.one('SELECT CommandId FROM scheduler.Command WHERE CommandId=?',cmd['id']):
                        db.execute("INSERT scheduler.Command(CommandId,SiteId,EntryId,MachineId,WorkerEpoch,Kind,Status,IsOpen,ConfigVersion,PayloadJson,NotBeforeUtc,ExpiresUtc) VALUES(?,1,?,1,?,?,?,?,?,'{}',?,?)",cmd['id'],e['id'],self.epoch,cmd['kind'],cmd['status'],is_open,cmd['version'],utc(cmd['due']),utc(cmd['due']+timedelta(minutes=3)))
                    db.execute('UPDATE scheduler.Command SET Status=?,IsOpen=?,ResultCode=?,UpdatedUtc=SYSUTCDATETIME() WHERE CommandId=?',cmd['status'],is_open,cmd.get('result'),cmd['id'])
            active_accounts={e['account'] for e in entries if e['status'] not in ('closed','failed')}
            for aid in active_accounts:
                if not db.one('SELECT BotAccountId FROM scheduler.AccountLease WHERE BotAccountId=?',aid):
                    db.execute('INSERT scheduler.AccountLease(BotAccountId,SiteId,MachineId,WorkerEpoch,LeaseToken,LeaseUntilUtc,LastConfirmedUtc) VALUES(?,1,1,?,?,DATEADD(second,90,SYSUTCDATETIME()),SYSUTCDATETIME())',aid,self.epoch,str(uuid.uuid4()))
                db.execute('UPDATE scheduler.AccountLease SET LeaseUntilUtc=DATEADD(second,90,SYSUTCDATETIME()),LastConfirmedUtc=SYSUTCDATETIME(),RequiresReconciliation=? WHERE BotAccountId=? AND WorkerEpoch=?',any(e['account']==aid and e['status']=='uncertain' for e in entries),aid,self.epoch)
            # A cancelled first join has no session yet, but still owns a lease.
            # Retain leases for uncertain/open entries; release only this worker's
            # leases once the persisted journal confirms no entries remain.
            db.execute('DELETE l FROM scheduler.AccountLease l WHERE l.SiteId=1 AND l.WorkerEpoch=? AND NOT EXISTS (SELECT 1 FROM scheduler.Entry e WHERE e.BotAccountId=l.BotAccountId AND e.IsOpen=1)',self.epoch)
            from .rules import split_interval
            for span in intervals:
                if db.one('SELECT IntervalId FROM scheduler.AccountInterval WHERE IntervalId=?',span['id']): continue
                db.execute('INSERT scheduler.AccountInterval(IntervalId,SiteId,BotAccountId,MachineId,WorkerEpoch,SequenceNumber,StartUtc,EndUtc,EntryCount) VALUES(?,1,?,1,?,?,?,?,?)',span['id'],span['account'],self.epoch,span['sequence'],utc(span['start']),utc(span['end']),span['entries'])
                for day,seconds in split_interval(span['start'],span['end'],'Europe/Budapest').items():
                    count=int(seconds)
                    if not count: continue
                    if db.one('SELECT LocalDate FROM scheduler.AccountDay WHERE SiteId=1 AND BotAccountId=? AND LocalDate=?',span['account'],day):
                        db.execute('UPDATE scheduler.AccountDay SET SessionSeconds=SessionSeconds+?,EntrySeconds=EntrySeconds+?,UpdatedUtc=SYSUTCDATETIME() WHERE SiteId=1 AND BotAccountId=? AND LocalDate=?',count,count*span['entries'],span['account'],day)
                    else:
                        db.execute('INSERT scheduler.AccountDay(SiteId,BotAccountId,LocalDate,SessionSeconds,EntrySeconds) VALUES(1,?,?,?,?)',span['account'],day,count,count*span['entries'])

    def live_view_requests(self):
        """Return only unexpired requests assigned to this worker's machine."""
        with self.transaction() as db:
            db.execute("UPDATE scheduler.LiveViewRequest SET Status='expired' WHERE MachineId=1 AND Status IN ('requested','active') AND ExpiresUtc<=SYSUTCDATETIME()")
            return db.rows("""SELECT r.RequestId,r.TableId,r.BotAccountId
                FROM scheduler.LiveViewRequest r
                JOIN scheduler.Entry e ON e.SiteId=r.SiteId AND e.TableId=r.TableId
                    AND e.BotAccountId=r.BotAccountId AND e.MachineId=r.MachineId AND e.IsOpen=1
                WHERE r.SiteId=1 AND r.MachineId=1 AND r.Status IN ('requested','active')
                    AND r.ExpiresUtc>SYSUTCDATETIME()""")

    def publish_live_view(self,request_id,hand_id,payload):
        with self.transaction() as db:
            request=db.one("""SELECT RequestId FROM scheduler.LiveViewRequest WITH(UPDLOCK,HOLDLOCK)
                WHERE RequestId=? AND SiteId=1 AND MachineId=1 AND Status IN ('requested','active')
                    AND ExpiresUtc>SYSUTCDATETIME()""",request_id)
            if not request:
                return False
            if db.one('SELECT RequestId FROM scheduler.LiveTableSnapshot WHERE RequestId=?',request_id):
                db.execute("""UPDATE scheduler.LiveTableSnapshot SET SequenceNumber=SequenceNumber+1,
                    CapturedUtc=SYSUTCDATETIME(),HandId=?,PayloadJson=? WHERE RequestId=?""",str(hand_id or ''),json.dumps(payload),request_id)
            else:
                db.execute("""INSERT scheduler.LiveTableSnapshot(RequestId,SequenceNumber,CapturedUtc,HandId,PayloadJson)
                    VALUES(?,1,SYSUTCDATETIME(),?,?)""",request_id,str(hand_id or ''),json.dumps(payload))
            db.execute("UPDATE scheduler.LiveViewRequest SET Status='active',LastSnapshotUtc=SYSUTCDATETIME(),ErrorText=NULL WHERE RequestId=?",request_id)
            return True
