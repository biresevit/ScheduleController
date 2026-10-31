"""Immediate rollouts update every PC at once; agents too old to force-stop drain instead of hanging."""
from datetime import datetime, timedelta, timezone

from schedule_controller.update_rollouts import OLD_AGENT, finish_recovery, tick_rollouts

NOW = datetime.now(timezone.utc)


def task(mid, status='queued', version='1.0.13', **fields):
    now = NOW.replace(tzinfo=None)
    return dict(TaskId=f'task{mid}', RolloutId='rollout', SiteId=1, MachineId=mid, Position=mid, Status=status,
        IndependentInstall=False, Staged=status != 'queued', CurrentReleaseId='old', AgentEnabled=True,
        AgentHeartbeat=now, LastHeartbeatUtc=now, IsReady=True, AgentStage='resuming', AppsHealthy=True,
        AgentVersion=version, UpdatedUtc=now, OriginalEntries=11, **fields)


class Database:
    def __init__(self, tasks, immediate=True):
        self.tasks = tasks
        self.rollout = dict(RolloutId='rollout', ReleaseId='release', Immediate=immediate, Status='running')
        self.statements = []

    def rows(self, sql, *args):
        if 'SELECT DISTINCT CustomerId' in sql: return [{'CustomerId': 1}]
        if 'Immediate=1' in sql: return [self.rollout] if self.rollout['Immediate'] else []
        if 'SELECT * FROM scheduler.UpdateRollout' in sql: return []  # Idle-PC installations.
        if 'SELECT t.*' in sql: return self.tasks
        if 'ScheduleWindow' in sql:  # The whole day targets 20 entries.
            return [dict(Weekday=d, StartMinute=0, EndMinute=1440, TargetEntries=20) for d in range(7)]
        raise AssertionError(sql)

    def one(self, sql, *args):
        if 'sp_getapplock' in sql: return {'value': 0}
        if "Status='paused'" in sql: return None
        if 'SELECT TOP(1) * FROM scheduler.UpdateRollout' in sql:
            return None if self.rollout['Immediate'] else self.rollout
        if 'SELECT TOP(1) t.*' in sql: return self.tasks[0]
        if 'SELECT TOP(1) EntryId' in sql or 'SELECT TOP(1) SessionId' in sql: return None
        if 'SELECT TaskId' in sql: return None
        if 'COUNT(*) AS n' in sql: return {'n': 0}
        if 'scheduler.Site' in sql: return {'TimeZone': 'UTC', 'SchedulingEnabled': True}
        if 'SUM(CASE' in sql: return {'uncertain': 0, 'total': 2, 'accounts': 1}
        raise AssertionError(sql)

    def execute(self, sql, *args): self.statements.append((sql, args))

    def moved(self, status):
        return sorted(args[-1] for sql, args in self.statements
                      if f"SET Status='{status}'" in sql and 'MachineUpdateTask' in sql)


def test_immediate_rollout_starts_every_pc_at_once():
    db = Database([task(1), task(2), task(3)])
    tick_rollouts(db, NOW)
    assert db.moved('preparing') == ['task1', 'task2', 'task3']


def test_serial_rollout_still_starts_one_pc():
    db = Database([task(1), task(2), task(3)], immediate=False)
    tick_rollouts(db, NOW)
    assert db.moved('preparing') == ['task1']


def test_staged_pcs_force_stop_unless_the_agent_is_too_old():
    db = Database([task(1, 'preparing'), task(2, 'preparing', version='1.0.12')])
    tick_rollouts(db, NOW)
    assert db.moved('forcing') == ['task1']
    assert db.moved('draining') == ['task2']
    assert any(OLD_AGENT in args for sql, args in db.statements)


def test_old_agent_stuck_in_forcing_falls_back_to_draining():
    db = Database([task(1, 'forcing', version='1.0.12'), task(2, 'forcing')])
    tick_rollouts(db, NOW)
    assert db.moved('draining') == ['task1']


def test_immediate_pc_completes_without_waiting_for_traffic():
    old = (NOW - timedelta(minutes=2)).replace(tzinfo=None)
    for immediate, done in ((True, True), (False, False)):
        db = Database([])
        db.rollout.update(Immediate=immediate, ReleaseId='old')
        finish_recovery(db, task(1, 'resuming') | {'UpdatedUtc': old}, db.rollout, NOW)
        assert bool(db.moved('completed')) is done


def test_completion_tolerates_slow_worker_publishes():
    old = (NOW - timedelta(minutes=2)).replace(tzinfo=None)
    db = Database([])
    db.rollout.update(Immediate=True, ReleaseId='old')
    slow = task(1, 'resuming') | {'UpdatedUtc': old, 'AppsHealthy': False,
                                  'LastHeartbeatUtc': (NOW - timedelta(seconds=100)).replace(tzinfo=None)}
    finish_recovery(db, slow, db.rollout, NOW)
    assert db.moved('completed')
    db = Database([])
    db.rollout.update(Immediate=True, ReleaseId='old')
    finish_recovery(db, slow | {'IsReady': False}, db.rollout, NOW)  # A stopped worker never completes.
    assert not db.moved('completed')
