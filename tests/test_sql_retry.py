from contextlib import contextmanager
import pytest
from schedule_controller.sql_retry import retry_deadlocks


def test_deadlock_retries_the_whole_transaction_after_rollback(monkeypatch):
    monkeypatch.setattr('schedule_controller.sql_retry.time.sleep',lambda _:None)
    events=[]
    @contextmanager
    def transaction():
        events.append('begin')
        try:yield
        except Exception:events.append('rollback');raise
        else:events.append('commit')
    @retry_deadlocks
    def operation():
        with transaction():
            if len(events)==1:raise RuntimeError('40001','SQL Server deadlock victim (1205)')
            events.append('write')
            return 42
    assert operation()==42
    assert events==['begin','rollback','begin','write','commit']


def test_unknown_connection_failure_is_not_replayed(monkeypatch):
    calls=[]
    @retry_deadlocks
    def operation():calls.append(1);raise RuntimeError('08S01','Connection interrupted')
    with pytest.raises(RuntimeError):operation()
    assert len(calls)==1


def test_deadlock_retry_is_bounded(monkeypatch):
    monkeypatch.setattr('schedule_controller.sql_retry.time.sleep',lambda _:None)
    calls=[]
    @retry_deadlocks
    def operation():calls.append(1);raise RuntimeError('40001','deadlock (1205)')
    with pytest.raises(RuntimeError):operation()
    assert len(calls)==6
