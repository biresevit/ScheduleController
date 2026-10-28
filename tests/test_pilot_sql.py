"""Pilot journal tests use the real schema but roll back every mutation."""
from contextlib import contextmanager
from datetime import datetime,timedelta,timezone
import os
import uuid
import pytest
from schedule_controller.pilot_repository import Repository,DB
from schedule_controller.database import connection_string

pytestmark=pytest.mark.skipif(os.getenv('SCHEDULE_SQL_TEST')!='1',reason='Opt-in rollback SQL tests')

@pytest.fixture
def repo():
    import pyodbc
    conn=pyodbc.connect(connection_string(),autocommit=False)
    r=Repository(str(uuid.uuid4()))
    @contextmanager
    def transaction(): yield DB(conn.cursor())
    r.transaction=transaction
    try: yield r
    finally: conn.rollback();conn.close()


def test_pilot_journal_lifecycle_and_daily_accounting(repo):
    c=repo.configuration();aid=next(a['BotAccountId'] for a in c['accounts'] if a['Username']=='monica')
    repo.start(30)
    stamp=datetime.now(timezone.utc)
    table='pilot-test-'+str(uuid.uuid4())
    tables=[dict(id=table,name='Rollback fixture',game='NLH',bb=2,minimum=40,maximum=400,seats=6,occupied=1,open=True,observed=stamp,snapshot=stamp)]
    seats={table:[dict(seat=0,native_id='test',name='monica',account=aid,stack=100,bet=2,in_game=True,away=False,hand='test',observed=stamp)]}
    sid=str(uuid.uuid4());eid=str(uuid.uuid4())
    sessions=[dict(id=sid,account=aid,start=stamp,end=stamp+timedelta(minutes=30))]
    cmd=dict(id=str(uuid.uuid4()),kind='join',status='succeeded',version=c['site']['ConfigVersion'],due=stamp)
    entries=[dict(id=eid,account=aid,table=table,status='seated',created=stamp,seated=stamp,confirmed=stamp,session=sid,commands=[cmd])]
    interval=dict(id=str(uuid.uuid4()),account=aid,sequence=1,start=stamp,end=stamp+timedelta(seconds=30),entries=3)
    repo.publish(tables,seats,entries,sessions,[interval])
    repo.publish(tables,seats,entries,sessions,[interval]) # retry must not double-account
    with repo.transaction() as db:
        assert db.one('SELECT COUNT(*) AS n FROM scheduler.AccountInterval WHERE IntervalId=?',interval['id'])['n']==1
        assert db.one('SELECT IsOpen FROM scheduler.Entry WHERE EntryId=?',eid)['IsOpen']
        assert db.one('SELECT WorkerEpoch FROM scheduler.AccountLease WHERE BotAccountId=?',aid)
    entries[0].update(status='closed',closed=stamp+timedelta(seconds=30))
    sessions[0].update(closed=stamp+timedelta(seconds=30),reason='test')
    repo.publish(tables,seats,entries,sessions,[])
    with repo.transaction() as db:
        assert not db.one('SELECT IsOpen FROM scheduler.Entry WHERE EntryId=?',eid)['IsOpen']
        assert db.one('SELECT BotAccountId FROM scheduler.AccountLease WHERE BotAccountId=?',aid) is None


def test_cancelled_first_join_releases_lease_without_a_session(repo):
    c=repo.configuration()
    aid=next(a['BotAccountId'] for a in c['accounts'] if a['Username']=='monica')
    repo.start(30)
    stamp=datetime.now(timezone.utc)
    entry=dict(id=str(uuid.uuid4()),account=aid,table='cancelled-test',status='reserved',created=stamp)
    table=dict(id='cancelled-test',name='Rollback fixture',game='NLH',bb=2,minimum=40,maximum=400,seats=6,occupied=0,open=True,observed=stamp,snapshot=stamp)
    repo.publish([table],{},[entry],[],[])
    with repo.transaction() as db:
        assert db.one('SELECT BotAccountId FROM scheduler.AccountLease WHERE BotAccountId=?',aid)
    entry.update(status='closed',closed=stamp)
    repo.publish([],{},[entry],[],[])
    with repo.transaction() as db:
        assert db.one('SELECT BotAccountId FROM scheduler.AccountLease WHERE BotAccountId=?',aid) is None


def test_temporary_schedule_restores_and_stale_epoch_cannot_write(repo):
    original=repo.configuration()
    repo.start(30)
    stamp=datetime.now(timezone.utc)
    saved=repo.prepare_schedule(stamp,stamp+timedelta(minutes=30),24,True)
    c=repo.configuration()
    assert all(a['StakeTier']=='high' for a in c['accounts'])
    assert c['windows'][0]['TargetEntries']==24
    repo.restore(saved)
    restored=repo.configuration()
    assert {a['Username']:a['StakeTier'] for a in restored['accounts']}=={a['Username']:a['StakeTier'] for a in original['accounts']}
    assert not restored['site']['SchedulingEnabled']
    repo.epoch=str(uuid.uuid4())
    with pytest.raises(RuntimeError,match='epoch'):
        repo.publish([],{},[],[],[])


def test_account_absence_reconciles_all_uncertain_entries(repo):
    c=repo.configuration();aid=next(a['BotAccountId'] for a in c['accounts'] if a['Username']=='monica')
    repo.start(30)
    # Keep the fixture independent of small client/SQL-server clock skew.
    stamp=datetime.now(timezone.utc)-timedelta(minutes=1);sid=str(uuid.uuid4())
    sessions=[dict(id=sid,account=aid,start=stamp,end=stamp+timedelta(minutes=30))]
    entries=[];tables=[]
    for n in range(2):
        tid='reconcile-'+str(uuid.uuid4())
        tables.append(dict(id=tid,name='Rollback fixture',game='NLH',bb=2,minimum=40,maximum=400,seats=6,occupied=1,open=True,observed=stamp,snapshot=stamp))
        entries.append(dict(id=str(uuid.uuid4()),account=aid,table=tid,status='uncertain',created=stamp,session=sid,
                            commands=[dict(id=str(uuid.uuid4()),kind='leave',status='uncertain',version=c['site']['ConfigVersion'],due=stamp,result='cleanup_unconfirmed')]))
    repo.publish(tables,{},entries,sessions,[])
    repo.restore({'site':{'ConfigVersion':-1},'accounts':[],'bands':[],'windows':[]})
    reconciled=repo.confirm_account_absent(aid)
    assert {str(x).lower() for x in reconciled}=={e['id'] for e in entries}
    with repo.transaction() as db:
        assert db.one('SELECT COUNT(*) AS n FROM scheduler.Entry WHERE BotAccountId=? AND IsOpen=1',aid)['n']==0
        assert db.one('SELECT BotAccountId FROM scheduler.AccountLease WHERE BotAccountId=?',aid) is None
