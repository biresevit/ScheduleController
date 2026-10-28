import asyncio
from datetime import datetime,timedelta,timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
import random

from schedule_controller.pilot import Pilot,Session
from schedule_controller.planner import Settings
from schedule_controller.rules import Band


def test_early_timer_does_not_push_cooldown_forward(monkeypatch):
    import schedule_controller.pilot as module
    stamp=datetime(2026,10,26,17,tzinfo=timezone.utc)
    p=Pilot.__new__(Pilot)
    p.configuration=AsyncMock();p.publish=AsyncMock()
    p.config={'site':{'SchedulingEnabled':True,'TimeZone':'UTC','ConfigVersion':1},
              'machine':{'MaxEntries':30,'IsEnabled':True},
              'windows':[{'Weekday':0,'StartMinute':0,'EndMinute':1440,'TargetEntries':1}]}
    p.policies={1:{'StakeTier':'high','PlayingFrequency':'high','PlayedSeconds':0,'MaxPlaytimeMinutes':240,
                   'MaxTables':3,'IsActive':True,'IsEnabled':True}}
    p.clients={1:SimpleNamespace(failures={})};p.sessions=[];p.entries={};p.seats={'42':[]}
    p.tables=[dict(id='42',bb=Decimal(2),seats=6,occupied=0,snapshot=stamp,game='NLH')]
    bands=[Band('low',Decimal('.02'),Decimal('.5'),0),Band('medium',Decimal('.5'),Decimal(1),0),Band('high',Decimal(1),Decimal(4),10000,True)]
    p.settings=Settings(1,bands);p.args=SimpleNamespace(capacity=30);p.rng=random.Random(7)
    p.next_join=p.next_leave=None;p.pending=[];p.last_plan=stamp-timedelta(seconds=119)
    monkeypatch.setattr(module,'now',lambda:stamp)
    asyncio.run(p.schedule())
    assert p.last_plan==stamp-timedelta(seconds=119)
    assert not p.entries
    monkeypatch.setattr(module,'now',lambda:stamp+timedelta(seconds=2))
    asyncio.run(p.schedule())
    assert len(p.entries)==1
    assert p.last_plan==stamp+timedelta(seconds=2)


def test_run_it_twice_is_sent_only_when_offer_is_handled(monkeypatch):
    session=Session.__new__(Session)
    session.policy={'Username':'monica'}
    session.queues={42:object()}
    session.client=SimpleNamespace(player_command=AsyncMock())
    monkeypatch.setattr(asyncio,'sleep',AsyncMock())
    asyncio.run(session.respond_run_it_twice(42,True,3.5))
    session.client.player_command.assert_awaited_once_with(42,17)


def test_expired_run_it_twice_offer_does_not_fail_table(monkeypatch):
    session=Session.__new__(Session)
    session.policy={'Username':'monica'}
    session.queues={42:object()}
    session.client=SimpleNamespace(player_command=AsyncMock(side_effect=RuntimeError('offer expired')))
    monkeypatch.setattr(asyncio,'sleep',AsyncMock())
    asyncio.run(session.respond_run_it_twice(42,False,2))


def test_restart_adopts_an_existing_seat_without_buying_in(monkeypatch):
    import schedule_controller.pilot as module
    class FakeBot:
        async def process(self,state): pass
    monkeypatch.setattr(module,'Bot',lambda *args,**kwargs:FakeBot())
    state={'s':[{'f':1}]}
    client=SimpleNamespace(tables={},states={42:state},join=AsyncMock(),hero_seat=lambda value:0)
    session=Session.__new__(Session)
    session.client=client;session.decision=object();session.bots={};session.queues={};session.workers={}
    async def exercise():
        assert await session.adopt(42,{'i':42,'n':'Existing table'})=='sitting_out'
        client.join.assert_awaited_once_with(42)
        assert 42 in session.bots and 42 in session.workers
        session.workers[42].cancel()
        await asyncio.gather(session.workers[42],return_exceptions=True)
    asyncio.run(exercise())


def test_restart_closes_an_absent_seat_without_starting_a_worker():
    client=SimpleNamespace(tables={},states={42:{'s':[]}},join=AsyncMock(),hero_seat=lambda value:None)
    session=Session.__new__(Session)
    session.client=client;session.decision=object();session.bots={};session.queues={};session.workers={}
    assert asyncio.run(session.adopt(42,{'i':42,'n':'Old table'})) is None
    assert session.workers=={}


def test_idle_session_reconnects_with_same_username(monkeypatch):
    import schedule_controller.pilot as module
    made=[]
    class FakeConnection:
        def __init__(self): self.listeners=[]
    class FakeClient:
        def __init__(self,settings):
            self.settings=settings;self.connection=FakeConnection();self.open=AsyncMock()
            made.append(self)
    old=SimpleNamespace(close=AsyncMock())
    session=Session.__new__(Session)
    session.policy={'Username':'kate'};session.client=old
    session.stamp=lambda message: None
    monkeypatch.setattr(module,'EvenBetClient',FakeClient)
    monkeypatch.setattr(asyncio,'sleep',AsyncMock())
    assert asyncio.run(session.reconnect_idle())
    old.close.assert_awaited_once()
    assert session.client is made[0]
    assert session.client.settings.username=='kate'
    session.client.open.assert_awaited_once()


def test_refresh_fails_over_to_second_observer():
    first=SimpleNamespace(policy={'Username':'monica'},failures={},ready=asyncio.Event())
    first.ready.set()
    first.client=SimpleNamespace(connection=SimpleNamespace(failure=None),list_tables=AsyncMock(side_effect=ConnectionError()),tables={},player_id=1)
    second=SimpleNamespace(policy={'Username':'kate'},failures={},ready=asyncio.Event(),observed={})
    second.ready.set()
    second.client=SimpleNamespace(connection=SimpleNamespace(failure=None),list_tables=AsyncMock(return_value=[]),tables={},
                                  state_versions={},states={},player_id=2,join=AsyncMock(),wait_state=AsyncMock())
    p=Pilot.__new__(Pilot)
    p.clients={1:first,2:second};p.entries={};p.info={};p.tables=[];p.seats={}
    asyncio.run(p.refresh())
    first.client.list_tables.assert_awaited_once()
    second.client.list_tables.assert_awaited_once()
    assert p.info=={}


def test_live_view_reports_current_acting_seat():
    captured={}
    class Repo:
        def live_view_requests(self):
            return [{'RequestId':'request-1','TableId':'42','BotAccountId':1}]
        def publish_live_view(self,request_id,hand_id,payload):
            captured.update(request_id=request_id,hand_id=hand_id,payload=payload)
            return True
    state={'gi':123,'m':{'ai':0,'di':1,'r':0},'d':{'c':'','p':300},'s':[
        {'i':10,'dn':'opponent','c':9900,'b':100,'d':'-1;-1','f':0},
        {'i':20,'dn':'monica','c':9800,'b':200,'d':'8;9','f':0}]}
    client=SimpleNamespace(states={42:state},tables={42:{'n':'Test table'}},hero_seat=lambda value:1)
    p=Pilot.__new__(Pilot)
    p.repo=Repo();p.clients={1:SimpleNamespace(client=client,policy={'Username':'monica'})}
    assert asyncio.run(p.publish_live_views())==1
    assert captured['payload']['acting_seat']==0
    assert captured['payload']['dealer']==1


def test_account_session_opens_on_play_task_and_closes_after_last_entry(monkeypatch):
    import schedule_controller.pilot as module
    events=[]
    class FakeSession:
        def __init__(self,policy,decision,rng):
            self.policy=policy
        async def open(self,purpose='play task'):
            events.append(('open',self.policy['Username'],purpose))
        async def close(self):
            events.append(('close',self.policy['Username']))
    monkeypatch.setattr(module,'Session',FakeSession)
    p=Pilot.__new__(Pilot)
    p.client_lock=asyncio.Lock();p.clients={};p.entries={}
    p.policies={1:{'Username':'monica'}};p.decision=object();p.rng=random.Random(1)
    async def exercise():
        first=await p.session_for(1)
        assert await p.session_for(1) is first
        assert await p.close_session_if_idle(1)
    asyncio.run(exercise())
    assert events==[('open','monica','play task'),('close','monica')]


def test_lobby_refresh_uses_one_temporary_login_then_logs_out(monkeypatch):
    import schedule_controller.pilot as module
    events=[]
    class Connection:
        failure=None
    class Client:
        connection=Connection();tables={};states={};state_versions={};player_id=10
        list_tables=AsyncMock(return_value=[])
    class FakeSession:
        def __init__(self,policy,decision,rng):
            self.policy=policy;self.client=Client();self.failures={};self.ready=asyncio.Event();self.ready.set();self.observed={}
        async def open(self,purpose='play task'): events.append(('open',self.policy['Username'],purpose))
        async def close(self): events.append(('close',self.policy['Username']))
    monkeypatch.setattr(module,'Session',FakeSession)
    p=Pilot.__new__(Pilot)
    p.clients={};p.entries={};p.info={};p.tables=[];p.seats={}
    p.policies={1:{'Username':'monica'},2:{'Username':'kate'}};p.decision=object();p.rng=random.Random(1)
    asyncio.run(p.refresh())
    assert events==[('open','monica','lobby scan'),('close','monica')]
    assert p.clients=={}
