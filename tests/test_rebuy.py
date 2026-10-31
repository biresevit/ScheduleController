"""A bot that loses its stack rebuys with the account's ring chance, otherwise it leaves the table."""
import asyncio
from datetime import datetime,timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
import json
import random

import pytest

from schedule_controller.pilot import Pilot,Session


def busted_session(chance,chips=0):
    session=Session.__new__(Session);session.repo=None
    session.policy={'Username':'monica','RebuyRingChance':chance,'PostBbChance':0,
                    'BuyInPolicyJson':json.dumps({'100':100})}
    state={'s':[{'i':42,'c':chips,'b':0}]}
    session.client=SimpleNamespace(states={7:state},tables={7:{'bb':200}},hero_seat=lambda s:0,
        buy_in_range=AsyncMock(return_value={'minAmount':4000,'maxAmount':40000}),rebuy=AsyncMock())
    session.bots={7:SimpleNamespace(leaving=False,busted=True)};session.failures={}
    session.ready=asyncio.Event();session.ready.set();session.rng=random.Random(3)
    session.results=[];session.on_bust_result=lambda tid,amount:session.results.append((tid,amount))
    return session


@pytest.fixture(autouse=True)
def no_delay(monkeypatch):
    monkeypatch.setattr(asyncio,'sleep',AsyncMock())


def test_rebuy_uses_the_accounts_buy_in_weights():
    session=busted_session(1)
    asyncio.run(session.rebuy_or_leave(7))
    session.client.rebuy.assert_awaited_once_with(7,'200.00',wait_big_blind=True)
    assert session.results==[(7,Decimal('200.00'))]


def test_declined_rebuy_leaves_the_table():
    session=busted_session(0)
    asyncio.run(session.rebuy_or_leave(7))
    session.client.rebuy.assert_not_awaited()
    assert session.results==[(7,None)]


def test_failed_rebuy_leaves_the_table():
    session=busted_session(1);session.client.rebuy.side_effect=TimeoutError()
    asyncio.run(session.rebuy_or_leave(7))
    assert session.results==[(7,None)]


def test_chips_credited_late_or_a_leave_in_progress_change_nothing():
    session=busted_session(1,chips=500)
    asyncio.run(session.rebuy_or_leave(7))
    assert session.results==[] and not session.bots[7].busted
    session=busted_session(1);session.bots[7].leaving=True
    asyncio.run(session.rebuy_or_leave(7))
    assert session.results==[]
    session.client.rebuy.assert_not_awaited()


def test_rebuy_chance_is_rolled_per_bust():
    rolls=[]
    for seed in range(200):
        session=busted_session(.7);session.rng=random.Random(seed)
        asyncio.run(session.rebuy_or_leave(7))
        rolls.append(session.results[0][1] is not None)
    assert 120<sum(rolls)<160


def pilot_with_entry(status='seated'):
    p=Pilot.__new__(Pilot)
    p.config={'site':{'ConfigVersion':3}};p.jobs={}
    e={'id':'e1','account':1,'table':'7','status':status,'buy_in':Decimal('100'),'commands':[]}
    p.entries={'e1':e}
    return p,e


def test_pilot_adds_a_rebuy_to_the_entry_buy_in():
    p,e=pilot_with_entry()
    p.bust_result(1,7,Decimal('80'))
    assert e['buy_in']==Decimal('180') and e['status']=='seated'


def test_pilot_leaves_after_a_declined_rebuy():
    p,e=pilot_with_entry()
    started=[]
    p.execute_serialized=lambda entry,cmd,kind:started.append((entry['id'],cmd['status'],kind)) or asyncio.sleep(0)
    async def run():
        p.bust_result(1,7,None)
        await asyncio.gather(*p.jobs.values())
    asyncio.run(run())
    assert e['status']=='draining' and started==[('e1','claimed','leave')]


def test_scheduler_leave_for_an_entry_the_worker_already_closed_is_a_no_op():
    p,e=pilot_with_entry('closed')
    p.publish=AsyncMock();p.clients={}
    cmd={'id':'c1','kind':'leave','status':'claimed'}
    asyncio.run(p.execute(e,cmd,'leave'))
    assert cmd['status']=='succeeded' and cmd['result']=='already_closed' and e['status']=='closed'


def test_live_view_shows_the_winner_and_shown_cards():
    captured={}
    class Repo:
        def live_view_requests(self):return [{'RequestId':'r','TableId':'42','BotAccountId':1}]
        def manual_snapshot(self,*args):return {}
        def publish_live_view(self,request_id,hand_id,payload):captured.update(payload);return True
    state={'gi':9,'ts':3,'m':{'r':6,'di':0,'ai':255},'d':{'c':'18;12;7;34;6','p':0},'s':[
        {'i':10,'dn':'shown','c':9900,'b':0,'d':'17;36','f':0},
        {'i':11,'dn':'mucked','c':9900,'b':0,'d':'-1;-1','f':0},
        {'i':20,'dn':'monica','c':9800,'b':0,'d':'8;9','f':0}]}
    winners=[{'seat':0,'name':'shown','amount':10.07,'hand':'Two Pair 6 3','text':'shown wins (10.07) with Two Pair 6 3'}]
    client=SimpleNamespace(states={42:state},tables={42:{'n':'T','g':72}},hero_seat=lambda s:2,results={42:{'hand_id':9,'winners':winners}})
    p=Pilot.__new__(Pilot)
    p.repo=Repo();p.clients={1:SimpleNamespace(client=client,bots={},policy={'Username':'monica'})}
    asyncio.run(p.publish_live_views())
    assert [s['cards'] for s in captured['seats']]==[['6d','jc'],[],['4c','4d']]
    assert captured['result']=={'hand_id':'9','winners':winners}


def test_buy_in_falls_back_to_the_only_legal_amount_when_rejoining():
    session=busted_session(1)
    session.client.buy_in_range=AsyncMock(return_value={'minAmount':39293,'maxAmount':39293})
    assert asyncio.run(session.buy_in_amount(7,random.Random(1)))==Decimal('392.93')


def test_buy_in_falls_back_to_the_closest_legal_amount():
    session=busted_session(1)
    session.policy['BuyInPolicyJson']=json.dumps({'20':5,'100':65,'200':20,'irregular':10})
    session.client.buy_in_range=AsyncMock(return_value={'minAmount':41000,'maxAmount':45000})  # 205–225 BB at BB 2
    assert asyncio.run(session.buy_in_amount(7,random.Random(1)))==Decimal('410.00')
