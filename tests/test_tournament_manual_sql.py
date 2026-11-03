"""Real SQL binding/lease checks on connection-local tables; no live rows touched."""
import json
import os
from contextlib import contextmanager
import uuid

import pytest

from schedule_controller.database import connection_string
from schedule_controller.pilot_repository import DB, Repository

pytestmark=pytest.mark.skipif(os.getenv('SCHEDULE_SQL_TEST')!='1',reason='Opt-in isolated SQL fixtures')


def test_table_move_closes_window_expires_actions_and_never_rebinds_control():
    import pyodbc
    conn=pyodbc.connect(connection_string(),autocommit=False)
    names=['MachineAssignment','LiveViewRequest','LiveTableSnapshot','ManualTableControl','ManualTableAction','TableActionClaim','Entry']
    class TempDB(DB):
        def bind(self,sql,args):
            for name in names:sql=sql.replace('scheduler.'+name,'#'+name)
            return super().bind(sql,args)
    repo=Repository(str(uuid.uuid4()),99123,99456)
    @contextmanager
    def transaction():yield TempDB(conn.cursor(),repo.site_id,repo.machine_id)
    repo.transaction=transaction
    db=TempDB(conn.cursor(),repo.site_id,repo.machine_id)
    try:
        db.execute('CREATE TABLE #MachineAssignment(SiteId int,MachineId int,WorkerEpoch uniqueidentifier)')
        db.execute('CREATE TABLE #LiveViewRequest(RequestId uniqueidentifier,SiteId int,MachineId int,BotAccountId int,TableId nvarchar(80),Status varchar(10),ExpiresUtc datetime2,LastSnapshotUtc datetime2,ErrorText nvarchar(400))')
        db.execute('CREATE TABLE #LiveTableSnapshot(RequestId uniqueidentifier,SequenceNumber bigint,CapturedUtc datetime2,HandId nvarchar(80),PayloadJson nvarchar(max))')
        db.execute('CREATE TABLE #ManualTableControl(SiteId int,MachineId int,BotAccountId int,TableId nvarchar(80),WorkerEpoch uniqueidentifier,RequestId uniqueidentifier,Enabled bit,UpdatedUtc datetime2)')
        db.execute('CREATE TABLE #ManualTableAction(ActionId uniqueidentifier,SiteId int,MachineId int,BotAccountId int,TableId nvarchar(80),WorkerEpoch uniqueidentifier,RequestId uniqueidentifier,TurnKey char(64),Action varchar(10),Status varchar(12),ResultText nvarchar(500),UpdatedUtc datetime2,CreatedUtc datetime2)')
        db.execute('CREATE TABLE #TableActionClaim(SiteId int,BotAccountId int,TableId nvarchar(80),TurnKey char(64),Source varchar(10),CreatedUtc datetime2)')
        db.execute('CREATE TABLE #Entry(EntryId uniqueidentifier,SiteId int,MachineId int,BotAccountId int,TableId nvarchar(80),PlayTableId nvarchar(80),IsOpen bit,WorkerEpoch uniqueidentifier)')
        aid=99777;rid=str(uuid.uuid4());action_id=str(uuid.uuid4());key='a'*64
        db.execute('INSERT #MachineAssignment VALUES(:site,:machine,?)',repo.epoch)
        db.execute("INSERT #Entry VALUES(NEWID(),:site,:machine,?,'T123','42',1,?)",aid,repo.epoch)
        db.execute("INSERT #LiveViewRequest VALUES(?,:site,:machine,?,'T123','requested',DATEADD(minute,5,SYSUTCDATETIME()),NULL,NULL)",rid,aid)
        assert repo.publish_live_view(rid,1,{'table_id':'42','tournament':True,'amount_unit':'tournament_chips'})
        db.execute("INSERT #ManualTableControl VALUES(:site,:machine,?,'42',?,?,1,SYSUTCDATETIME())",aid,repo.epoch,rid)
        db.execute("INSERT #ManualTableAction VALUES(?,:site,:machine,?,'42',?,?,?,'raise','queued',NULL,SYSUTCDATETIME(),SYSUTCDATETIME())",action_id,aid,repo.epoch,rid,key)
        mode,command=repo.manual_offer(aid,42,key)
        assert mode and str(command['ActionId']).lower()==action_id
        # Database already knows the new table, even before the window sees the move.
        db.execute("UPDATE #Entry SET PlayTableId='43'")
        assert not repo.claim_table_action(aid,42,key,command)
        assert not repo.publish_live_view(rid,2,{'table_id':'43','tournament':True})
        row=db.one('SELECT Status,ErrorText FROM #LiveViewRequest WHERE RequestId=?',rid)
        assert row['Status']=='failed' and 'new live view' in row['ErrorText']
        assert not db.one('SELECT Enabled FROM #ManualTableControl')['Enabled']
        assert db.one('SELECT Status FROM #ManualTableAction')['Status']=='expired'
        snapshot=db.one('SELECT PayloadJson,SequenceNumber FROM #LiveTableSnapshot WHERE RequestId=?',rid)
        assert json.loads(snapshot['PayloadJson'])['table_id']=='42' and snapshot['SequenceNumber']==1
        # A new window binds to the new table, with manual control initially off.
        new_rid=str(uuid.uuid4())
        db.execute("INSERT #LiveViewRequest VALUES(?,:site,:machine,?,'T123','requested',DATEADD(minute,5,SYSUTCDATETIME()),NULL,NULL)",new_rid,aid)
        assert repo.publish_live_view(new_rid,2,{'table_id':'43','tournament':True})
        assert repo.manual_offer(aid,43,key)==(False,None)
        assert not repo.publish_live_view(rid,3,{'table_id':'42','tournament':True})
    finally:
        conn.rollback();conn.close()
