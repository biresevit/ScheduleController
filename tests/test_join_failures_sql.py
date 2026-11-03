"""Exercise the production SQL against connection-local fixtures, never live rows."""
import os
import pytest

from schedule_controller.database import connection_string
from schedule_controller.join_failures import limit_failed_sessions

pytestmark=pytest.mark.skipif(os.getenv('SCHEDULE_SQL_TEST')!='1',reason='Opt-in isolated SQL fixtures')


def test_failed_session_limit_cancels_queued_work_and_preserves_live_seats():
    import pyodbc
    conn=pyodbc.connect(connection_string(),autocommit=False)
    class DB:
        def execute(self,sql,*args):
            for table in ['AccountSession','AccountLease','Entry','Command']:
                sql=sql.replace('scheduler.'+table,'#'+table)
            return conn.cursor().execute(sql,*args)
    db=DB()
    try:
        db.execute('CREATE TABLE #AccountSession(SessionId int,SiteId int,BotAccountId int,StartedUtc datetime2,PlannedEndUtc datetime2,ActualEndUtc datetime2,IsOpen bit,EndReason varchar(80))')
        db.execute('CREATE TABLE #Entry(EntryId int,SessionId int,BotAccountId int,Status varchar(20),IsOpen bit,ClosedUtc datetime2)')
        db.execute('CREATE TABLE #Command(EntryId int,Kind varchar(12),Status varchar(20),IsOpen bit,ResultCode varchar(80),ResultText nvarchar(1000),UpdatedUtc datetime2)')
        db.execute('CREATE TABLE #AccountLease(SiteId int,BotAccountId int,RequiresReconciliation bit)')
        # One failure, two failures + queued join, two failures + existing seat,
        # two failures + claimed join, another site, and an uncertain seat.
        for sid in range(1,7):
            db.execute("INSERT #AccountSession VALUES(?,?,?,DATEADD(hour,-1,SYSUTCDATETIME()),DATEADD(hour,1,SYSUTCDATETIME()),NULL,1,NULL)",sid,2 if sid==5 else 1,sid)
            db.execute('INSERT #AccountLease VALUES(?,?,?)',2 if sid==5 else 1,sid,sid==6)
            for i in range(1 if sid==1 else 2):
                eid=sid*10+i
                db.execute("INSERT #Entry VALUES(?,?,?,'failed',0,SYSUTCDATETIME())",eid,sid,sid)
                db.execute("INSERT #Command VALUES(?,'join','failed',0,'KeyError',NULL,SYSUTCDATETIME())",eid)
        for sid,status,command in [(2,'reserved','pending'),(3,'seated','succeeded'),(4,'joining','claimed'),(6,'uncertain','uncertain')]:
            eid=sid*10+5
            db.execute('INSERT #Entry VALUES(?,?,?,?,1,NULL)',eid,sid,sid,status)
            db.execute("INSERT #Command VALUES(?,'join',?,?,NULL,NULL,SYSUTCDATETIME())",eid,command,command in ('pending','claimed','uncertain'))
        limit_failed_sessions(db,1)
        rows={r.SessionId:r for r in conn.cursor().execute('SELECT * FROM #AccountSession').fetchall()}
        assert rows[1].IsOpen and rows[1].EndReason is None
        assert not rows[2].IsOpen and rows[2].EndReason=='join_failure_limit'
        assert all(rows[sid].IsOpen and rows[sid].EndReason=='join_failure_limit' for sid in [3,4,6])
        assert rows[5].EndReason is None
        assert conn.cursor().execute('SELECT Status FROM #Command WHERE EntryId=25').fetchone()[0]=='cancelled'
        assert conn.cursor().execute('SELECT Status FROM #Entry WHERE EntryId=25').fetchone()[0]=='closed'
        assert conn.cursor().execute('SELECT COUNT(*) FROM #AccountLease WHERE BotAccountId=2').fetchone()[0]==0
        assert conn.cursor().execute('SELECT COUNT(*) FROM #AccountLease WHERE BotAccountId IN (3,4,6)').fetchone()[0]==3
        # Idempotent and no accidental extension of the forced end.
        end=rows[3].PlannedEndUtc
        limit_failed_sessions(db,1)
        assert conn.cursor().execute('SELECT PlannedEndUtc FROM #AccountSession WHERE SessionId=3').fetchone()[0]==end
    finally:
        conn.rollback();conn.close()
