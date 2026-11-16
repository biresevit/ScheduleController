"""Run stale-ownership cleanup against connection-local SQL fixtures only."""
import os
import pytest

from schedule_controller.database import connection_string
from schedule_controller.mtt_sessions import release_stale_idle_leases

pytestmark=pytest.mark.skipif(os.getenv('SCHEDULE_SQL_TEST')!='1',reason='Opt-in isolated SQL fixtures')


def test_stale_leases_with_only_parked_registrations_are_released_after_reconciliation():
    import pyodbc
    conn=pyodbc.connect(connection_string(),autocommit=False,timeout=15)
    conn.timeout=30
    class DB:
        def execute(self,sql,*args):
            for table in ('AccountLease','MachineAssignment','Entry'):
                sql=sql.replace('scheduler.'+table,'#'+table)
            return conn.cursor().execute(sql,*args)
    db=DB()
    try:
        db.execute('CREATE TABLE #AccountLease(BotAccountId int,SiteId int,MachineId int,WorkerEpoch varchar(10),RequiresReconciliation bit)')
        db.execute('CREATE TABLE #MachineAssignment(SiteId int,MachineId int,WorkerEpoch varchar(10),IsReady bit,LastHeartbeatUtc datetime2)')
        db.execute('CREATE TABLE #Entry(BotAccountId int,IsOpen bit,IsParked bit)')
        # 1 parked, 2 no entries, 3 active, 4 uncertain active, 5 current epoch,
        # 6 still reconciling, 7 offline, 8 another site, 9 parked plus active.
        for aid in range(1,10):
            sid=2 if aid==8 else 1
            db.execute('INSERT #AccountLease VALUES(?,?,?,?,1)',aid,sid,aid,'new' if aid==5 else 'old')
            db.execute('INSERT #MachineAssignment VALUES(?,?,\'new\',?,DATEADD(second,?,SYSUTCDATETIME()))',
                       sid,aid,0 if aid==6 else 1,-300 if aid==7 else 0)
            if aid!=2:db.execute('INSERT #Entry VALUES(?,1,?)',aid,0 if aid in (3,4) else 1)
        db.execute('INSERT #Entry VALUES(9,1,0)')
        assert release_stale_idle_leases(db,1)==2
        left={r[0] for r in conn.cursor().execute('SELECT BotAccountId FROM #AccountLease')}
        assert left=={3,4,5,6,7,8,9}
        assert release_stale_idle_leases(db,1)==0
    finally:
        conn.rollback();conn.close()
