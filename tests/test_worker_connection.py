import sys
import threading
from types import SimpleNamespace

import pytest

from schedule_controller import pilot_repository
from schedule_controller.pilot_repository import Repository


class Cursor:
    def __init__(self,conn):self.conn=conn;self.description=[('x',)]
    def setinputsizes(self,sizes):self.conn.sizes.append(sizes)
    def execute(self,sql,*args):self.conn.statements.append(sql);return self
    def fetchall(self):return [(1,)]
    def close(self):pass


class Conn:
    def __init__(self):self.statements=[];self.sizes=[];self.commits=0;self.rollbacks=0;self.closed=False
    def cursor(self):return Cursor(self)
    def commit(self):self.commits+=1
    def rollback(self):self.rollbacks+=1
    def close(self):self.closed=True


@pytest.fixture
def opened(monkeypatch):
    conns=[]
    def connect(*args,**kwargs):
        conns.append(Conn());return conns[-1]
    monkeypatch.setitem(sys.modules,'pyodbc',SimpleNamespace(connect=connect))
    monkeypatch.setattr(pilot_repository,'connection_string',lambda:'DRIVER=x')
    monkeypatch.setattr(pilot_repository,'_local',threading.local())
    clock=[1000.0]
    monkeypatch.setattr(pilot_repository.time,'monotonic',lambda:clock[0])
    return conns,clock


def test_worker_transactions_reuse_one_connection_and_set_session_options_once(opened):
    conns,_=opened
    repo=Repository('epoch',1,5)
    for _ in range(5):
        with repo.transaction() as db:db.rows('SELECT 1 WHERE :site=:machine')
    assert len(conns)==1
    conn=conns[0]
    assert sum('XACT_ABORT' in s for s in conn.statements)==1
    assert conn.commits==6  # session setup + five transactions
    assert not conn.closed


def test_a_failed_transaction_discards_its_connection(opened):
    conns,_=opened
    repo=Repository('epoch',1,5)
    with pytest.raises(RuntimeError):
        with repo.transaction() as db:
            db.rows('SELECT 1');raise RuntimeError('boom')
    assert conns[0].rollbacks==1 and conns[0].closed
    with repo.transaction() as db:db.rows('SELECT 1')
    assert len(conns)==2 and not conns[1].closed


def test_idle_and_old_connections_are_replaced(opened):
    conns,clock=opened
    repo=Repository('epoch',1,5)
    with repo.transaction():pass
    clock[0]+=pilot_repository.CONNECTION_IDLE_SECONDS+1
    with repo.transaction():pass
    assert len(conns)==2 and conns[0].closed
    for _ in range(int(pilot_repository.CONNECTION_MAX_AGE_SECONDS//100)+1):
        clock[0]+=100
        with repo.transaction():pass
    assert len(conns)==3 and conns[1].closed


def test_each_thread_has_its_own_connection(opened):
    conns,_=opened
    repo=Repository('epoch',1,5)
    def work():
        with repo.transaction():pass
    threads=[threading.Thread(target=work) for _ in range(3)]
    for t in threads:t.start()
    for t in threads:t.join()
    with repo.transaction():pass
    assert len(conns)==4


def test_string_parameters_get_fixed_declarations_so_plans_are_reused(opened):
    conns,_=opened
    repo=Repository('epoch',1,5)
    with repo.transaction() as db:
        db.execute('SELECT ?,?,?,?,:site',pilot_repository.Text('{"a":1}'),'short','x'*5000,7)
    sizes=conns[0].sizes[-1]
    assert sizes==[(pilot_repository.SQL_WVARCHAR,0,0),(pilot_repository.SQL_WVARCHAR,4000,0),(pilot_repository.SQL_WVARCHAR,0,0),None,None]


def test_journal_payload_is_always_nvarchar_max():
    payload=pilot_repository.journal_payload([],[],[])
    assert payload and all(isinstance(p,pilot_repository.Text) for p in payload)
