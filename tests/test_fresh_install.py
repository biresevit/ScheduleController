from datetime import datetime, timezone
from schedule_controller.update_rollouts import tick_rollouts


class EnrollmentDatabase:
    def __init__(self,status='queued',open_session=False):
        self.now=datetime.now(timezone.utc)
        self.open_session=open_session
        self.statements=[]
        self.task=dict(TaskId='task',SiteId=1,MachineId=7,Status=status,CurrentReleaseId=None,
            AgentEnabled=True,AgentHeartbeat=self.now.replace(tzinfo=None),LastHeartbeatUtc=None,
            IsReady=False,AgentStage='awaiting_install' if status=='queued' else 'staged',
            Staged=status!='queued',UpdatedUtc=self.now.replace(tzinfo=None))
    def rows(self,sql,*args):
        if 'SELECT DISTINCT CustomerId' in sql:return [{'CustomerId':1}]
        if 'SELECT * FROM scheduler.UpdateRollout' in sql:return []
        raise AssertionError(sql)
    def one(self,sql,*args):
        if 'sp_getapplock' in sql:return {'value':0}
        if 'SELECT SiteId FROM scheduler.Site' in sql:return {'SiteId':1}
        if "Status='paused'" in sql:return None
        if 'SELECT TOP(1) * FROM scheduler.UpdateRollout' in sql:return {'RolloutId':'rollout','ReleaseId':'release'}
        if 'SELECT TOP(1) t.*' in sql:return self.task
        if 'SELECT TOP(1) EntryId' in sql:return None
        if 'SELECT TOP(1) SessionId' in sql:return {'SessionId':'open'} if self.open_session else None
        if 'SELECT TaskId' in sql:return None
        if 'COUNT(*) AS n' in sql:return {'n':0}
        raise AssertionError(sql)
    def execute(self,sql,*args):self.statements.append((sql,args))


def test_new_agent_can_stage_without_application_heartbeat():
    db=EnrollmentDatabase()
    tick_rollouts(db,db.now)
    assert any("Status='preparing'" in sql for sql,_ in db.statements)


def test_staged_new_machine_installs_without_draining_nonexistent_worker():
    db=EnrollmentDatabase('preparing')
    tick_rollouts(db,db.now)
    assert any("Status='stopping'" in sql for sql,_ in db.statements)
    assert any('MaintenanceTaskId=?' in sql for sql,_ in db.statements)


def test_fresh_install_claim_cannot_bypass_open_sessions():
    db=EnrollmentDatabase(open_session=True)
    tick_rollouts(db,db.now)
    assert db.statements==[]
