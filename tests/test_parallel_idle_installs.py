from datetime import datetime,timedelta,timezone
import pytest
from schedule_controller.update_rollouts import tick_idle_installations,tick_rollouts


class Database:
    def __init__(self):
        self.now=datetime.now(timezone.utc)
        self.tasks=[];self.statements=[];self.entries=set();self.sessions=set();self.active=set()
    def task(self,mid,**values):
        t=dict(TaskId=f'task-{mid}',SiteId=1,MachineId=mid,RolloutId='rollout',
            Status='queued',IndependentInstall=False,CurrentReleaseId=None,
            AgentEnabled=True,AgentHeartbeat=self.now.replace(tzinfo=None),LastHeartbeatUtc=None,
            IsReady=False,AgentStage='awaiting_install',MaintenanceTaskId=None,
            Staged=False,UpdatedUtc=self.now.replace(tzinfo=None))
        t.update(values);self.tasks.append(t);return t
    def rows(self,sql,*args):
        if 'Immediate=1' in sql:return []
        if 'FROM scheduler.UpdateRollout' in sql:return [{'RolloutId':'rollout','ReleaseId':'release'}]
        if 'FROM scheduler.MachineUpdateTask t' in sql:return self.tasks
        raise AssertionError(sql)
    def one(self,sql,*args):
        if 'SELECT SiteId FROM scheduler.Site' in sql:return {'SiteId':1}
        if 'SELECT TOP(1) EntryId' in sql:return {'EntryId':'open'} if args[0] in self.entries else None
        if 'SELECT TOP(1) SessionId' in sql:return {'SessionId':'open'} if args[0] in self.sessions else None
        if 'SELECT TaskId' in sql:return {'TaskId':'other'} if args[0] in self.active else None
        raise AssertionError(sql)
    def execute(self,sql,*args):self.statements.append((sql,args))
    def started(self):return [args[-1] for sql,args in self.statements if 'IndependentInstall=1' in sql]


def test_multiple_new_pcs_install_while_another_pc_is_busy():
    db=Database();db.active.add(99);db.task(16);db.task(17)
    tick_idle_installations(db,1,db.now)
    assert db.started()==['task-16','task-17']
    assert sum('MaintenanceTaskId=?' in sql for sql,_ in db.statements)==2


def test_existing_ready_idle_pc_also_bypasses_traffic_queue():
    db=Database();db.active.add(99)
    db.task(16,CurrentReleaseId='old',IsReady=True,LastHeartbeatUtc=db.now.replace(tzinfo=None))
    tick_idle_installations(db,1,db.now)
    assert db.started()==['task-16']


def test_stopped_installed_pc_can_receive_repair_when_its_journal_is_empty():
    db=Database();db.task(16,CurrentReleaseId='old',AppsHealthy=False,AgentStage='rolled_back')
    tick_idle_installations(db,1,db.now)
    assert db.started()==['task-16']


def test_stopped_pc_cannot_skip_open_entries_or_failed_operation_review():
    db=Database();t=db.task(16,CurrentReleaseId='old',AppsHealthy=False,AgentStage='staged')
    db.entries.add(16)
    tick_idle_installations(db,1,db.now)
    assert not db.started()
    db.entries.clear();t['AgentStage']='failed'
    tick_idle_installations(db,1,db.now)
    assert not db.started()


@pytest.mark.parametrize('blocker',['entry','session','stale_agent','offline_worker','same_machine_update','other_maintenance'])
def test_idle_install_never_bypasses_unknown_or_open_work(blocker):
    db=Database();t=db.task(16)
    if blocker=='entry':db.entries.add(16)  # includes reserved and uncertain entries
    if blocker=='session':db.sessions.add(16)
    if blocker=='stale_agent':t['AgentHeartbeat']-=timedelta(minutes=4)
    if blocker=='offline_worker':t['CurrentReleaseId']='old'
    if blocker=='same_machine_update':db.active.add(16)
    if blocker=='other_maintenance':t['MaintenanceTaskId']='other'
    tick_idle_installations(db,1,db.now)
    assert not db.started()


def test_staged_idle_install_rechecks_journal_and_never_drains_live_sessions():
    db=Database();db.task(16,Status='preparing',IndependentInstall=True,Staged=True,AgentStage='staged',MaintenanceTaskId='task-16')
    db.entries.add(16)
    tick_idle_installations(db,1,db.now)
    assert db.statements==[]
    db.entries.clear()
    tick_idle_installations(db,1,db.now)
    assert any("Status='stopping'" in sql for sql,_ in db.statements)
    assert not any("Status='draining'" in sql for sql,_ in db.statements)


def test_unrelated_paused_rollout_does_not_block_a_new_pc():
    class PausedDatabase(Database):
        def rows(self,sql,*args):
            if 'SELECT DISTINCT CustomerId' in sql:return [{'CustomerId':1}]
            return super().rows(sql,*args)
        def one(self,sql,*args):
            if 'sp_getapplock' in sql:return {'value':0}
            if "Status='paused'" in sql:return {'RolloutId':'paused-other-pc'}
            return super().one(sql,*args)
    db=PausedDatabase();db.task(17)
    tick_rollouts(db,db.now)
    assert db.started()==['task-17']


@pytest.mark.parametrize('other_is_idle',[True,False])
def test_only_another_traffic_update_blocks_a_busy_pc(other_is_idle):
    class BusyDatabase(Database):
        def rows(self,sql,*args):
            if 'SELECT DISTINCT CustomerId' in sql:return [{'CustomerId':1}]
            return super().rows(sql,*args)
        def one(self,sql,*args):
            if 'sp_getapplock' in sql:return {'value':0}
            if "Status='paused'" in sql:return None
            if 'SELECT TOP(1) * FROM scheduler.UpdateRollout' in sql:return {'RolloutId':'rollout','ReleaseId':'release'}
            if 'SELECT TOP(1) t.*' in sql:return self.tasks[0]
            if 'IndependentInstall=0 AND TaskId<>' in sql:return None if other_is_idle else {'TaskId':'other-busy'}
            if 'COUNT(*) AS n' in sql:return {'n':2}
            return super().one(sql,*args)
    db=BusyDatabase();db.task(16,IsReady=True,LastHeartbeatUtc=db.now.replace(tzinfo=None),CurrentReleaseId='old')
    db.entries.add(16)
    tick_rollouts(db,db.now)
    prepared=any("Status='preparing'" in sql for sql,_ in db.statements)
    assert prepared==other_is_idle


def test_a_rollout_is_not_completed_while_an_idle_pc_is_still_installing():
    from schedule_controller.update_rollouts import complete_if_done
    db=Database();complete_if_done(db,'rollout',db.now.replace(tzinfo=None))
    sql,args=db.statements[-1]
    # the completion is guarded by every task of the rollout, independent idle installs included
    assert "Status='completed'" in sql and 'NOT EXISTS' in sql and 'IndependentInstall' not in sql
    assert args[1:]==('rollout','rollout')
