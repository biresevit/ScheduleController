from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
import asyncio
import json
import random

from schedule_controller.mtt_sessions import waits_for_mtt, wake_registrations
from schedule_controller.pilot_repository import journal_payload
from schedule_controller.pilot import Pilot

NOW = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)


def test_only_far_future_registering_mtts_wait_offline():
    table={'table_type':'MTT','tournament_state':'registering','start':NOW+timedelta(hours=2)}
    assert waits_for_mtt(table,NOW)
    assert not waits_for_mtt({**table,'start':NOW+timedelta(minutes=29)},NOW)
    assert not waits_for_mtt({**table,'tournament_state':'running'},NOW)
    assert not waits_for_mtt({**table,'table_type':'SNG'},NOW)


class DB:
    def __init__(self):self.writes=[]
    def execute(self,sql,*args):self.writes.append((sql,args))
    def rows(self,sql,*args):
        if 'FROM scheduler.Entry e WITH' in sql:
            return [dict(EntryId='entry',BotAccountId=7,TableId='T123',MaxTables=6,SessionMinMinutes=60,
                         SessionMaxMinutes=240,OpenSessionId=None,SessionMachine=None)]
        if 'FROM scheduler.MachineAssignment' in sql:
            return [dict(MachineId=5,WorkerEpoch='epoch',Used=0,Capacity=10)]
        raise AssertionError(sql)
    def one(self,sql,*args):
        if 'COUNT(*)' in sql:return {'n':0}
        if 'AccountLease' in sql:return None
        raise AssertionError(sql)


def test_registered_bots_do_not_wake_on_a_full_machine():
    class FullDB(DB):
        def rows(self, sql, *args):
            rows = super().rows(sql, *args)
            if 'FROM scheduler.MachineAssignment' in sql:
                for row in rows:
                    row['Used'] = row['Capacity']
            return rows

    db = FullDB()
    site = dict(SiteId=3, NextMttLoginUtc=None, MinJoinGapSeconds=20, MaxJoinGapSeconds=60, ConfigVersion=1)
    def create(*args):
        raise AssertionError('A full machine must not start another playing session')
    assert wake_registrations(db, site, NOW, random.Random(1), create) == 0
    assert not any('INSERT scheduler.Command' in sql for sql, args in db.writes)


def test_return_creates_a_new_playing_session_and_resume_task_without_registering():
    db=DB();sessions=[]
    def create(*args):sessions.append(args);return 'new-session'
    site=dict(SiteId=3,NextMttLoginUtc=None,MinJoinGapSeconds=20,MaxJoinGapSeconds=60,ConfigVersion=1)
    assert wake_registrations(db,site,NOW,random.Random(1),create)==1
    assert len(sessions)==1
    command=next(args for sql,args in db.writes if 'INSERT scheduler.Command' in sql)
    assert json.loads(command[6])['resume_registration'] is True
    assert 'registration_only' not in json.loads(command[6])
    scheduled=next(args[0] for sql,args in db.writes if 'SET NextMttLoginUtc' in sql)
    assert NOW.replace(tzinfo=None)+timedelta(seconds=20)<=scheduled<=NOW.replace(tzinfo=None)+timedelta(seconds=60)
    second=DB()
    assert wake_registrations(second,{**site,'NextMttLoginUtc':scheduled+timedelta(seconds=30)},NOW,random.Random(1),create)==0
    assert len(sessions)==1


def test_parked_entries_keep_registration_but_hold_no_account_lease_or_session():
    entry=dict(id='e',account=7,table='T123',status='joining',created=NOW,parked=True,session=None)
    payload=journal_payload([entry],[],[])
    # The SQL payload keeps the entry open, but no login lease is renewed.
    assert json.loads(payload[3])==[]
    assert json.loads(payload[1])[0]['parked'] is True
    assert json.loads(payload[1])[0]['session'] is None


def test_worker_restarts_with_parked_registration_without_logging_in():
    async def go():
        fake=SimpleNamespace(repo=SimpleNamespace(park_registered_entry=lambda eid:None),entries={},
                             session_for=AsyncMock())
        row=dict(EntryId='e',BotAccountId=7,TableId='T123',Status='joining',CreatedUtc=NOW,
                 IsParked=True,LastConfirmedUtc=NOW,SessionId=None)
        await Pilot.reconcile_account(fake,7,[row],AsyncMock())
        fake.session_for.assert_not_called()
        assert fake.entries['e']['parked'] and fake.entries['e']['session'] is None
    asyncio.run(go())
