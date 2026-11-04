import asyncio
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import random
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from schedule_controller.join_failures import LIMIT_REASON, failure_description
from schedule_controller.pilot import Pilot
from schedule_controller.pilot_repository import Repository


def fixture_pilot(failures=0):
    p=Pilot.__new__(Pilot)
    p.cloud=True;p.end=datetime.now(timezone.utc)+timedelta(hours=2)
    p.errors=[];p.info={42:{'n':'Ring'}};p.rng=random.Random(1)
    p.policies={7:{'Username':'andrew'}};p.accrue=lambda:None
    p.sessions=[dict(id='session',account=7,start=p.end-timedelta(hours=2),end=p.end)]
    p.entries={};p.execution_locks={};p.client_lock=asyncio.Lock()
    p.config={'site':{'ConfigVersion':1}}
    p.decision=SimpleNamespace(require_hand_history=AsyncMock())
    client=SimpleNamespace(tables={42:{'n':'Ring'}},list_tables=AsyncMock())
    c=SimpleNamespace(policy={'Username':'andrew'},failures={},client=client,
        add=AsyncMock(side_effect=KeyError(42)),remove=AsyncMock(),close=AsyncMock(),
        tourneys=SimpleNamespace(register=AsyncMock(side_effect=RuntimeError('Registration denied'))))
    p.clients={7:c};p.session_for=AsyncMock(return_value=c)
    persisted={f'old{i}' for i in range(failures)}
    def block(sid):return LIMIT_REASON if len(persisted)>=2 else None
    p.repo=SimpleNamespace(join_session_block=block,authorize_join=lambda version:True,
                           enforce_join_failure_limit=lambda:None)
    async def publish():
        persisted.update(cmd['id'] for e in p.entries.values() for cmd in e['commands']
                         if cmd.get('status')=='failed' and cmd['kind']=='join')
    p.publish=publish
    return p,c,persisted


def entry(p,i,table='42'):
    cmd=dict(id=f'c{i}',kind='join',version=1,status='claimed')
    e=dict(id=f'e{i}',account=7,table=table,status='reserved',session='session',commands=[cmd])
    p.entries[e['id']]=e
    return e,cmd


def test_second_failure_cancels_claimed_third_attempt_and_logs_out():
    async def scenario():
        p,c,history=fixture_pilot()
        work=[entry(p,i) for i in range(3)]
        await asyncio.gather(*(p.execute_serialized(e,cmd,'join') for e,cmd in work))
        assert c.add.await_count==2
        assert [cmd['status'] for _,cmd in work]==['failed','failed','cancelled']
        assert work[-1][1]['result']==LIMIT_REASON
        assert len(history)==2 and p.sessions[0]['reason']==LIMIT_REASON
        c.close.assert_awaited_once()
        assert not p.clients
        assert 'andrew' in work[0][1]['result_text'] and '42' in work[0][1]['result_text']
        assert 'Missing table or response field' in work[0][1]['result_text']
    asyncio.run(scenario())


def test_ring_and_registration_failures_share_persisted_budget_after_restart():
    async def scenario():
        p,c,history=fixture_pilot(failures=1) # A failure persisted by an earlier worker.
        e,cmd=entry(p,2,'T123')
        p.tables=[dict(id='T123',table_type='SNG')]
        await p.execute_serialized(e,cmd,'join')
        assert cmd['status']=='failed' and 'registering for the tournament' in cmd['result_text']
        next_e,next_cmd=entry(p,3)
        await p.execute_serialized(next_e,next_cmd,'join')
        c.add.assert_not_awaited()
        assert next_cmd['status']=='cancelled' and len(history)==2
    asyncio.run(scenario())


def test_second_failure_drains_existing_ring_seat_but_preserves_tournament():
    async def scenario():
        p,c,_=fixture_pilot(failures=1)
        ring,_=entry(p,'ring','43');ring['status']='seated'
        tournament,_=entry(p,'tournament','T12');tournament['status']='seated'
        e,cmd=entry(p,2)
        p.start_leave=lambda row:row.update(status='draining')
        await p.execute_serialized(e,cmd,'join')
        assert ring['status']=='draining' and tournament['status']=='seated'
        c.close.assert_not_awaited()
    asyncio.run(scenario())


def test_lobby_refresh_during_login_does_not_remove_selected_table():
    async def scenario():
        p,c,_=fixture_pilot()
        c.add.side_effect=None;c.add.return_value=10
        async def login(aid):
            p.info={} # Refresh ran while authentication awaited.
            return c
        p.session_for=login
        e,cmd=entry(p,1)
        await p.execute_serialized(e,cmd,'join')
        assert cmd['status']=='succeeded' and e['status']=='seated'
        assert c.add.await_args.args[1]=={'n':'Ring'}
    asyncio.run(scenario())


def test_missing_observer_table_is_refreshed_from_logged_in_account():
    async def scenario():
        p,c,_=fixture_pilot();p.info={}
        c.add.side_effect=None;c.add.return_value=10
        e,cmd=entry(p,1)
        await p.execute_serialized(e,cmd,'join')
        c.client.list_tables.assert_awaited_once()
        assert cmd['status']=='succeeded'
    asyncio.run(scenario())


def test_repository_checks_session_outcomes_not_command_attempt_count():
    repo=Repository('epoch')
    row=dict(IsOpen=True,NotEnded=True,EndReason=None,Failures=1)
    @contextmanager
    def tx():yield SimpleNamespace(one=lambda *args:row)
    repo.transaction=tx
    assert repo.join_session_block('s') is None
    row['Failures']=2
    assert repo.join_session_block('s')==LIMIT_REASON
    row.update(Failures=0,IsOpen=False)
    assert repo.join_session_block('s')=='session_ended'


def test_failure_messages_retain_context_and_bound_length():
    text=failure_description(KeyError('minAmount'),'Join','andrew',42,'reading buy-in limits')
    assert all(s in text for s in ['andrew','42','minAmount','reading buy-in limits','KeyError'])
    assert len(failure_description(ValueError('x'*2000),'Registration','andrew','T1','registering'))==1000
