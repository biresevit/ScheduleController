from contextlib import contextmanager
import pytest
from schedule_controller.sql_retry import retry_deadlocks
from schedule_controller.sql_retry import is_transient_database_error
import pyodbc


@pytest.mark.parametrize('state,message,expected',[
    ('HYT00','Query timeout expired',True),
    ('HYT01','Login timeout expired',True),
    ('08S01','Communication link failure',True),
    ('40001','Deadlock victim (1205)',True),
    ('42000','Database unavailable (40613)',True),
    ('HY000','Lock request timeout (1222)',True),
    ('42000','Invalid object name (208)',False),
    ('23000','Duplicate key (2627)',False),
    ('28000','Login failed (18456)',False),
])
def test_transient_sql_error_classification(state,message,expected):
    assert is_transient_database_error(pyodbc.OperationalError(state,message)) is expected


def test_non_sql_failures_are_never_classified_as_database_outages():
    assert not is_transient_database_error(RuntimeError('HYT00','Query timeout expired'))
    assert not is_transient_database_error(ConnectionError('Poker socket disconnected'))


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
