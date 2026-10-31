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
    session=Session.__new__(Session);session.repo=None
    session.policy={'Username':'monica'}
    session.queues={42:object()}
    session.client=SimpleNamespace(player_command=AsyncMock())
    monkeypatch.setattr(asyncio,'sleep',AsyncMock())
    asyncio.run(session.respond_run_it_twice(42,True,3.5))
    session.client.player_command.assert_awaited_once_with(42,17)


def test_expired_run_it_twice_offer_does_not_fail_table(monkeypatch):
    session=Session.__new__(Session);session.repo=None
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
    client=SimpleNamespace(tables={},states={42:state},join=AsyncMock(),hero_seat=lambda value:0,return_from_away=AsyncMock(),player_command=AsyncMock())
    session=Session.__new__(Session);session.repo=None
    session.client=client;session.decision=object();session.bots={};session.queues={};session.workers={}
    async def exercise():
        assert await session.adopt(42,{'i':42,'n':'Existing table'})=='sitting_out'
        client.join.assert_awaited_once_with(42)
        client.return_from_away.assert_awaited_once_with(42)
        client.player_command.assert_awaited_once_with(42,10)
        assert 42 in session.bots and 42 in session.workers
        session.workers[42].cancel()
        await asyncio.gather(session.workers[42],return_exceptions=True)
    asyncio.run(exercise())


def test_restart_closes_an_absent_seat_without_starting_a_worker():
    client=SimpleNamespace(tables={},states={42:{'s':[]}},join=AsyncMock(),hero_seat=lambda value:None)
    session=Session.__new__(Session);session.repo=None
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
    session=Session.__new__(Session);session.repo=None
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
        def manual_snapshot(self,*args):return {"supported":True,"enabled":False}
        def publish_live_view(self,request_id,hand_id,payload):
            captured.update(request_id=request_id,hand_id=hand_id,payload=payload)
            return True
    state={'gi':123,'m':{'ai':0,'di':1,'r':0},'d':{'c':'','p':300},'s':[
        {'i':10,'dn':'opponent','c':9900,'b':100,'d':'-1;-1','f':0},
        {'i':20,'dn':'monica','c':9800,'b':200,'d':'8;9','f':0}]}
    client=SimpleNamespace(states={42:state},tables={42:{'n':'Test table'}},hero_seat=lambda value:1)
    p=Pilot.__new__(Pilot)
    p.repo=Repo();p.clients={1:SimpleNamespace(client=client,bots={},policy={'Username':'monica'})}
    assert asyncio.run(p.publish_live_views())==1
    assert captured['payload']['acting_seat']==0
    assert captured['payload']['dealer']==1


def test_account_session_opens_on_play_task_and_closes_after_last_entry(monkeypatch):
    import schedule_controller.pilot as module
    events=[]
    class FakeSession:
        def __init__(self,policy,decision,rng,repo=None):
            self.policy=policy
        async def open(self,purpose='play task'):
            events.append(('open',self.policy['Username'],purpose))
        async def close(self):
            events.append(('close',self.policy['Username']))
    monkeypatch.setattr(module,'Session',FakeSession)
    p=Pilot.__new__(Pilot)
    p.client_lock=asyncio.Lock();p.clients={};p.entries={};p.repo=None
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
        def __init__(self,policy,decision,rng,repo=None):
            self.policy=policy;self.client=Client();self.failures={};self.ready=asyncio.Event();self.ready.set();self.observed={}
        async def open(self,purpose='play task'): events.append(('open',self.policy['Username'],purpose))
        async def close(self): events.append(('close',self.policy['Username']))
    monkeypatch.setattr(module,'Session',FakeSession)
    p=Pilot.__new__(Pilot)
    p.clients={};p.entries={};p.info={};p.tables=[];p.seats={}
    p.policies={1:{'Username':'monica','BotAccountId':1},2:{'Username':'kate','BotAccountId':2}};p.decision=object();p.rng=random.Random(1)
    p.repo=SimpleNamespace(claim_observer=lambda:1,release_observer=lambda account:None)
    asyncio.run(p.refresh())
    assert events==[('open','monica','lobby scan'),('close','monica')]
    assert p.clients=={}


def test_idle_pc_does_not_log_in_another_machines_owned_account(monkeypatch):
    import schedule_controller.pilot as module
    p=Pilot.__new__(Pilot)
    p.clients={};p.entries={};p.policies={};p.info={};p.tables=[];p.seats={}
    p.repo=SimpleNamespace(claim_observer=lambda:None)
    monkeypatch.setattr(module,'Session',lambda *args:(_ for _ in ()).throw(AssertionError('No free observer should log in')))
    asyncio.run(p.refresh())
    assert p.tables==[] and p.info=={}


def test_refresh_closes_absent_journal_but_keeps_present_and_inflight(monkeypatch):
    import schedule_controller.pilot as module
    stamp=datetime(2026,10,29,17,tzinfo=timezone.utc);monkeypatch.setattr(module,'now',lambda:stamp)
    p=Pilot.__new__(Pilot);p.policies={1:{'Username':'kate'}};p.clients={};p.accrue=lambda:None;p.close_session_if_idle=AsyncMock();p.sessions=[{'id':'session','account':1,'start':stamp-timedelta(hours=1)}]
    p.tables=[{'id':'42','snapshot':stamp,'occupied':1}];p.seats={'42':[{'account':1,'name':'kate'}]}
    p.entries={'present':{'id':'present','account':1,'table':'42','status':'seated','created':stamp-timedelta(hours=1),'confirmed':stamp-timedelta(minutes=2)},'gone':{'id':'gone','account':1,'table':'43','status':'seated','created':stamp-timedelta(hours=1),'confirmed':stamp-timedelta(minutes=2)}}
    p.tables.append({'id':'43','snapshot':stamp,'occupied':0});p.seats['43']=[];p.jobs={}
    asyncio.run(p.reconcile_observed_entries())
    assert p.entries['present']['status']=='seated' and p.entries['present']['confirmed']==stamp
    assert p.entries['gone']['status']=='closed'
    p.entries['gone']['status']='draining';p.entries['gone']['confirmed']=stamp-timedelta(minutes=2)
    p.jobs={'gone':SimpleNamespace(done=lambda:False)}
    asyncio.run(p.reconcile_observed_entries());assert p.entries['gone']['status']=='draining'
    p.jobs={};p.tables[1]['snapshot']=stamp-timedelta(minutes=2)
    asyncio.run(p.reconcile_observed_entries());assert p.entries['gone']['status']=='draining'


def test_connection_observation_clocks_do_not_refresh_stale_clients():
    s=Session.__new__(Session);first={};second={}
    a=SimpleNamespace(connection=SimpleNamespace(listeners=[]));b=SimpleNamespace(connection=SimpleNamespace(listeners=[]))
    s.attach_observer(a,first);s.attach_observer(b,second)
    b.connection.listeners[0]({'t':'GameState','gameState':{'ti':42}})
    assert 42 in second and not first


def test_reconnect_keeps_surviving_tables_when_another_seat_is_absent(monkeypatch):
    import schedule_controller.pilot as module
    async def exercise():
        candidate=SimpleNamespace(connection=SimpleNamespace(listeners=[]),tables={},states={},open=AsyncMock(),close=AsyncMock())
        async def join(tid):
            candidate.states[tid]={'ti':tid,'s':[{'i':7}] if tid==42 else []}
            for listener in candidate.connection.listeners:listener({'t':'GameState','gameState':candidate.states[tid]})
        candidate.return_from_away=AsyncMock();candidate.player_command=AsyncMock();candidate.join=join;candidate.hero_seat=lambda state:0 if state['s'] else None
        monkeypatch.setattr(module,'EvenBetClient',lambda settings:candidate);monkeypatch.setattr(asyncio,'sleep',AsyncMock())
        session=Session.__new__(Session);session.repo=None;session.policy={'Username':'kate'};session.client=SimpleNamespace(tables={42:{},43:{}},close=AsyncMock());old=session.client
        session.queues={42:asyncio.Queue(),43:asyncio.Queue()};session.workers={};session.bots={tid:SimpleNamespace(client=old,process=AsyncMock()) for tid in (42,43)};session.failures={};session.observed={}
        assert await session.reconnect_active()
        assert set(session.queues)=={42} and set(session.bots)=={42}
        assert session.client is candidate and 43 in session.observed
        old.close.assert_awaited_once();candidate.close.assert_not_awaited()
        for task in session.workers.values():task.cancel()
        await asyncio.gather(*session.workers.values(),return_exceptions=True)
    asyncio.run(exercise())


def test_resume_rejection_cannot_abort_restart_reconciliation(monkeypatch):
    import schedule_controller.pilot as module
    from poker_controller.transport import ProtocolError
    monkeypatch.setattr(module,'Bot',lambda *a,**k:SimpleNamespace(process=AsyncMock()))
    session=Session.__new__(Session);session.repo=None;session.policy={'Username':'kate'};session.decision=object();session.bots={};session.queues={};session.workers={}
    session.client=SimpleNamespace(tables={},states={42:{'s':[{'f':0}]}},join=AsyncMock(),hero_seat=lambda state:0,player_command=AsyncMock(side_effect=ProtocolError({'t':'Result','text':'There is no such a player at the table.','resultCode':1})))
    async def exercise():
        assert await session.adopt(42,{})=='seated'
        for task in session.workers.values():task.cancel()
        await asyncio.gather(*session.workers.values(),return_exceptions=True)
    asyncio.run(exercise())


def test_live_views_publish_on_state_change_and_heartbeat_only_when_unchanged(monkeypatch):
    import schedule_controller.pilot as module
    clock=[100.0]
    monkeypatch.setattr(module.time,'monotonic',lambda:clock[0])
    versions={42:1}
    published=[]
    class Repo:
        def live_view_requests(self):return [{'RequestId':'r1','BotAccountId':1,'TableId':'42'}]
    p=Pilot.__new__(Pilot)
    p.repo=Repo()
    p.clients={1:SimpleNamespace(client=SimpleNamespace(state_versions=versions))}
    async def publish(request):
        published.append(versions[42]);return True
    p.publish_live_view=publish
    steps=iter([
        lambda:None,                                   # unchanged, inside heartbeat: skip
        lambda:versions.__setitem__(42,2),             # state changed: publish immediately
        lambda:clock.__setitem__(0,clock[0]+1.5),      # unchanged but heartbeat due: publish
    ])
    async def fake_sleep(_):
        try:next(steps)()
        except StopIteration:raise asyncio.CancelledError
    monkeypatch.setattr(module.asyncio,'sleep',fake_sleep)
    try:asyncio.run(p.run_live_views())
    except asyncio.CancelledError:pass
    assert published==[1,2,2]
