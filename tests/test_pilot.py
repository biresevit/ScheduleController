import asyncio
import json
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
        def __init__(self,platform,username,endpoint_url=None,**kw):
            self.settings=SimpleNamespace(username=username);self.connection=FakeConnection();self.open=AsyncMock()
            made.append(self)
    old=SimpleNamespace(close=AsyncMock())
    session=Session.__new__(Session);session.repo=None
    session.policy={'Username':'kate'};session.client=old;session.platform=module.EVENBET;session.endpoint=None
    session.stamp=lambda message: None
    monkeypatch.setattr(module,'create_client',FakeClient)
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
    p.clients={1:first,2:second};p.entries={};p.info={};p.tables=[];p.seats={};p.config={'site':{'Platform':'EvenBet'}}
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
        def __init__(self,policy,decision,rng,repo=None,site=None):
            self.policy=policy
        async def open(self,purpose='play task'):
            events.append(('open',self.policy['Username'],purpose))
        async def close(self):
            events.append(('close',self.policy['Username']))
    monkeypatch.setattr(module,'Session',FakeSession)
    p=Pilot.__new__(Pilot)
    p.client_lock=asyncio.Lock();p.clients={};p.entries={};p.repo=None;p.config={'site':{'Platform':'EvenBet'}}
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
        def __init__(self,policy,decision,rng,repo=None,site=None):
            self.policy=policy;self.client=Client();self.failures={};self.ready=asyncio.Event();self.ready.set();self.observed={}
        async def open(self,purpose='play task'): events.append(('open',self.policy['Username'],purpose))
        async def close(self): events.append(('close',self.policy['Username']))
    monkeypatch.setattr(module,'Session',FakeSession)
    p=Pilot.__new__(Pilot)
    p.clients={};p.entries={};p.info={};p.tables=[];p.seats={};p.config={'site':{'Platform':'EvenBet'}}
    p.policies={1:{'Username':'monica','BotAccountId':1},2:{'Username':'kate','BotAccountId':2}};p.decision=object();p.rng=random.Random(1)
    p.repo=SimpleNamespace(claim_observer=lambda:1,release_observer=lambda account:None)
    asyncio.run(p.refresh())
    assert events==[('open','monica','lobby scan'),('close','monica')]
    assert p.clients=={}


def test_expired_rest_credentials_do_not_prevent_a_fresh_lobby_observer(monkeypatch):
    import schedule_controller.pilot as module
    events=[]
    expired=_observer('expired-account',ConnectionError())
    expired.client.hub=SimpleNamespace(failed='CREDENTIAL_EXPIRED')

    class FreshSession:
        def __init__(self,policy,decision,rng,repo=None,site=None):
            self.policy=policy;self.failures={};self.ready=asyncio.Event();self.ready.set();self.observed={}
            self.client=SimpleNamespace(connection=SimpleNamespace(failure=None),tables={},states={},
                state_versions={},player_id=20,list_tables=AsyncMock(return_value=[]))
        async def open(self,purpose):events.append(('open',purpose))
        async def close(self):events.append(('close',))

    monkeypatch.setattr(module,'Session',FreshSession)
    p=Pilot.__new__(Pilot)
    p.clients={1:expired};p.entries={};p.info={};p.tables=[{'id':'stale'}];p.seats={}
    p.config={'site':{'Platform':'BluffPokerPlatform'}}
    p.policies={2:{'Username':'fresh-account','BotAccountId':2}};p.decision=object();p.rng=random.Random(1)
    p.repo=SimpleNamespace(machine_enabled=lambda:True,claim_observer=lambda:2,
                           release_observer=lambda account:events.append(('release',account)))
    asyncio.run(p.refresh())
    assert events==[('open','lobby scan'),('close',),('release',2)]
    expired.client.list_tables.assert_not_awaited()
    assert p.tables==[]


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
        monkeypatch.setattr(module,'create_client',lambda platform,username,**kw:candidate);monkeypatch.setattr(asyncio,'sleep',AsyncMock())
        session=Session.__new__(Session);session.repo=None;session.policy={'Username':'kate'};session.platform=module.EVENBET;session.endpoint=None;session.client=SimpleNamespace(tables={42:{},43:{}},close=AsyncMock());old=session.client
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


def test_cloud_worker_keeps_session_open_when_its_last_table_closes(monkeypatch):
    import schedule_controller.pilot as module
    stamp=datetime(2026,10,29,17,tzinfo=timezone.utc);monkeypatch.setattr(module,'now',lambda:stamp)
    def pilot(cloud):
        p=Pilot.__new__(Pilot);p.cloud=cloud;p.policies={1:{'Username':'kate'}};p.clients={};p.accrue=lambda:None
        p.close_session_if_idle=AsyncMock();p.jobs={};p.sessions=[{'id':'session','account':1,'start':stamp-timedelta(hours=1)}]
        p.tables=[{'id':'43','snapshot':stamp,'occupied':0}];p.seats={'43':[]}
        p.entries={'gone':{'id':'gone','account':1,'table':'43','status':'seated','created':stamp-timedelta(hours=1),'confirmed':stamp-timedelta(minutes=2)}}
        asyncio.run(p.reconcile_observed_entries())
        assert p.entries['gone']['status']=='closed'
        return p.sessions[0]
    assert not pilot(True).get('closed')
    assert pilot(False)['reason']=='reconciled_absent'


def test_tournament_leave_unregisters_only_when_the_operator_asked():
    def pilot(status):
        p=Pilot.__new__(Pilot)
        p.publish=AsyncMock();p.accrue=lambda:None;p.errors=[]
        p.policies={7:{'Username':'jeremy'}}
        tourneys=SimpleNamespace(unregister=AsyncMock())
        p.clients={7:SimpleNamespace(tourneys=tourneys,close=AsyncMock())}
        p.client_lock=asyncio.Lock()
        e={'id':'e1','account':7,'table':'T128872490','status':status,'commands':[]}
        p.entries={'e1':e}
        return p,e,tourneys
    p,e,tourneys=pilot('joining')
    cmd={'id':'c1','kind':'leave','payload':{}}
    asyncio.run(p.execute_tournament(e,cmd,'leave'))
    assert cmd['result']=='tournament_kept' and e['status']=='joining' and not tourneys.unregister.called
    cmd={'id':'c2','kind':'leave','payload':{'unregister':True,'manual':True}}
    asyncio.run(p.execute_tournament(e,cmd,'leave'))
    tourneys.unregister.assert_awaited_once_with(128872490)
    assert cmd['status']=='succeeded' and cmd['result']=='unregistered' and e['status']=='closed'
    # Nothing else open: the account logs out, as after its last ring table.
    assert 7 not in p.clients
    p,e,tourneys=pilot('seated')
    cmd={'id':'c3','kind':'leave','payload':{'unregister':True}}
    asyncio.run(p.execute_tournament(e,cmd,'leave'))
    assert cmd['status']=='failed' and 'already started' in cmd['result_text'] and e['status']=='seated'
    assert not tourneys.unregister.called


def test_bluffpoker_site_sessions_use_the_bluffpoker_client(tmp_path,monkeypatch):
    from poker_controller.bluffpoker import BluffPokerClient
    config=tmp_path/'bluffpoker.json'
    config.write_text(json.dumps({'api_key':'key','origin':'https://site.test','accounts':{'pepe':{'email':'p@test','password':'pw'}}}))
    monkeypatch.setenv('BLUFFPOKER_CONFIG',str(config))
    session=Session({'Username':'pepe'},object(),site={'Platform':'BluffPokerPlatform','EndpointUrl':'https://hub.test'})
    assert isinstance(session.client,BluffPokerClient) and session.client.settings.hub_url=='https://hub.test'
    from schedule_controller.bluffpoker_tournaments import BluffPokerTournaments
    assert isinstance(session.tourneys,BluffPokerTournaments)
    evenbet=Session({'Username':'kate'},object(),site={'Platform':'EvenBet'})
    assert type(evenbet.client).__name__=='EvenBetClient' and hasattr(evenbet,'tourneys')


def test_disabled_pc_logs_no_account_in_for_a_lobby_scan(monkeypatch):
    import schedule_controller.pilot as module
    p=Pilot.__new__(Pilot)
    p.clients={};p.entries={};p.policies={1:{'Username':'monica','BotAccountId':1}};p.info={};p.tables=[];p.seats={}
    p.repo=SimpleNamespace(machine_enabled=lambda:False,
        claim_observer=lambda:(_ for _ in ()).throw(AssertionError('A disabled PC must not claim a scan account')))
    monkeypatch.setattr(module,'Session',lambda *args,**kw:(_ for _ in ()).throw(AssertionError('No login on a disabled PC')))
    asyncio.run(p.refresh())
    assert p.tables==[] and p.info=={}


def test_plo5_tables_are_published_to_the_lobby():
    from schedule_controller.pilot import observed_ring_table
    assert observed_ring_table({'g':88,'p':85})  # PLO5 pot limit (BluffPoker), shown in Live Tables
    assert observed_ring_table({'g':72,'p':80}) and observed_ring_table({'g':79,'p':85})
    assert not observed_ring_table({'g':73,'p':80})


def _observer(name,error):
    c=SimpleNamespace(policy={'Username':name},failures={},ready=asyncio.Event(),observed={})
    c.ready.set()
    c.client=SimpleNamespace(connection=SimpleNamespace(failure=None),list_tables=AsyncMock(side_effect=error),tables={},player_id=name)
    return c


def test_a_failed_lobby_scan_keeps_the_site_and_its_last_snapshot():
    # Every observer failing used to raise 'No healthy lobby observer' and stop the site with all its seats.
    p=Pilot.__new__(Pilot)
    p.clients={1:_observer('monica',ConnectionError()),2:_observer('kate',ConnectionError())}
    p.entries={};p.info={'t':{}};p.tables=[{'id':'t'}];p.seats={'t':[]};p.config={'site':{'Platform':'BluffPoker'}}
    asyncio.run(p.refresh())
    assert p.tables==[{'id':'t'}] and p.seats=={'t':[]}


def test_a_rate_limited_lobby_scan_stops_at_the_first_account():
    from poker_controller.bluffpoker import BluffPokerError
    limited=BluffPokerError('RATE_LIMIT_EXCEEDED',status=429,topic='tables',uncertain=True)
    p=Pilot.__new__(Pilot)
    p.clients={1:_observer('monica',limited),2:_observer('kate',ConnectionError())}
    p.entries={};p.info={};p.tables=[];p.seats={};p.config={'site':{'Platform':'BluffPoker'}}
    asyncio.run(p.refresh())
    p.clients[1].client.list_tables.assert_awaited_once()
    p.clients[2].client.list_tables.assert_not_awaited()


def test_restart_reconciliation_lists_the_lobby_once_and_runs_accounts_together():
    running=[];peak=[0];listings=[]
    p=Pilot.__new__(Pilot)
    async def session_for(aid):
        async def list_tables():
            listings.append(aid)
        return SimpleNamespace(client=SimpleNamespace(list_tables=list_tables,tables={'7':{'i':'7','n':'Vienna'}}))
    async def reconcile_account(aid,entries,lobby_for):
        running.append(aid);peak[0]=max(peak[0],len(running))
        lobby=await lobby_for(await session_for(aid))
        assert 7 in lobby
        await asyncio.sleep(.01)
        running.remove(aid)
    p.session_for=session_for;p.reconcile_account=reconcile_account
    rows=[{'BotAccountId':a,'TableId':'7'} for a in range(1,13)]
    asyncio.run(p.reconcile_cloud_entries(rows))
    assert len(listings)==1 and peak[0]>1


def test_worker_lock_is_exclusive_and_released(tmp_path):
    from schedule_controller.pilot import WorkerLock
    first=WorkerLock(5,1006,tmp_path);second=WorkerLock(5,1006,tmp_path);other=WorkerLock(5,1,tmp_path)
    assert first.acquire()
    assert not second.acquire()  # a worker of this machine and site is still running
    assert other.acquire()  # another site of the PC has its own lock
    first.release()
    assert second.acquire()  # the previous worker is gone: its replacement need not wait out the heartbeat
    second.release();other.release()


def test_crashed_worker_lock_is_released_with_its_process(tmp_path):
    import subprocess,sys,textwrap
    from pathlib import Path
    code=textwrap.dedent(f"""
        import sys;sys.path.insert(0,r'{Path(__file__).resolve().parents[1]}')
        from schedule_controller.pilot import WorkerLock
        assert WorkerLock(9,9,r'{tmp_path}').acquire()
        print('held',flush=True)
        import os;os._exit(3)  # dies without releasing anything
    """)
    out=subprocess.run([sys.executable,'-c',code],capture_output=True,text=True,timeout=60)
    assert 'held' in out.stdout, out.stderr
    from schedule_controller.pilot import WorkerLock
    lock=WorkerLock(9,9,tmp_path)
    assert lock.acquire()
    lock.release()


def test_bluffpoker_offer_with_a_text_table_id_is_answered_inside_its_window(monkeypatch):
    table='3b355673-ab0e-44f8-92cc-88021c2b03b8'
    events=[{'t':'RunItTwiceOffer','tableId':table,'handSeq':42,'timeoutMs':5000}]
    async def next_event():
        if events: return events.pop()
        raise asyncio.CancelledError
    session=Session.__new__(Session);session.repo=None
    session.policy={'Username':'guido','RunItTwiceChance':Decimal(1)}
    session.queues={table:object()};session.rit_responses=set();session.rit_tasks=set()
    session.rng=random.Random(3)
    session.client=SimpleNamespace(connection=SimpleNamespace(next_event=next_event),states={},player_command=AsyncMock())
    delays=[]
    async def sleep(seconds): delays.append(seconds)
    monkeypatch.setattr(asyncio,'sleep',sleep)
    async def exercise():
        try: await session.receive()
        except asyncio.CancelledError: pass
        await asyncio.gather(*session.rit_tasks)
    asyncio.run(exercise())
    session.client.player_command.assert_awaited_once_with(table,17)
    assert session.rit_responses=={(table,42)} and 0.5<=delays[0]<=2.0


def test_command_for_an_account_added_after_start_reloads_the_configuration():
    session=Pilot.__new__(Pilot);session.execution_locks={};session.policies={}
    loaded={'n':0}
    async def configuration():
        loaded['n']+=1;session.policies={7:{'Username':'new_bot'}}
    session.configuration=configuration
    session.execute=AsyncMock();session.publish=AsyncMock()
    e={'id':'e1','account':7,'table':'T1','status':'reserved','commands':[]};cmd={'id':'c1','status':'claimed'}
    asyncio.run(session.execute_serialized(e,cmd,'join'))
    assert loaded['n']==1;session.execute.assert_awaited_once_with(e,cmd,'join')


def test_command_for_an_unknown_account_or_a_crash_never_stays_claimed():
    session=Pilot.__new__(Pilot);session.execution_locks={};session.policies={}
    async def configuration():pass
    session.configuration=configuration;session.publish=AsyncMock()
    e={'id':'e1','account':7,'table':'T1','status':'reserved','commands':[]};cmd={'id':'c1','status':'claimed'}
    asyncio.run(session.execute_serialized(e,cmd,'join'))
    assert cmd['status']=='failed' and cmd['result']=='unknown_account' and e['status']=='failed'
    session.policies={7:{'Username':'x'}};session.execute=AsyncMock(side_effect=KeyError(7))
    e={'id':'e2','account':7,'table':'T1','status':'joining','commands':[]};cmd={'id':'c2','status':'claimed'}
    asyncio.run(session.execute_serialized(e,cmd,'join'))
    assert cmd['status']=='failed' and e['status']=='failed' and session.publish.await_count==2


def test_decision_metrics_are_flushed_in_one_batch_and_kept_when_the_write_fails():
    from poker_controller.metrics import DecisionMetrics
    stamp=datetime(2026,11,13,21,37,tzinfo=timezone.utc)
    m=DecisionMetrics(clock=lambda:stamp)
    for ms in (120,480,2600):m.decision(1,3,ms)
    m.decision(4,1,700);m.missed('sit_out','Dean',42)
    p=Pilot.__new__(Pilot);p.decision=SimpleNamespace(metrics=m);p.policies={7:{'Username':'dean'}}
    written=[]
    p.repo=SimpleNamespace(publish_decision_metrics=lambda rows,events:written.append((rows,events)))
    asyncio.run(p.flush_metrics())
    rows,events=written[0]
    pre=next(r for r in rows if r['street']==1)
    assert pre['hour']==stamp.replace(minute=0) and pre['opponents']==3 and pre['decisions']==3
    assert pre['total_ms']==3200 and pre['max_ms']==2600 and pre['bands']==[1,1,0,0,1,0]
    assert events[0]['account']==7 and events[0]['kind']=='sit_out'
    def fail(rows,events):raise RuntimeError('db down')
    m.decision(2,2,300);p.repo=SimpleNamespace(publish_decision_metrics=fail)
    asyncio.run(p.flush_metrics())
    rows,_=m.drain();assert rows[0]['decisions']==1  # kept for the next batch


def test_restart_adoption_accepts_only_definitive_bluffpoker_closed_table():
    import pytest
    from poker_controller.bluffpoker import BluffPokerError
    from poker_controller.sites import BLUFFPOKER,EVENBET
    for platform,error,absent in [
        (BLUFFPOKER,BluffPokerError('TABLE_CLOSED',status=404),True),
        (BLUFFPOKER,BluffPokerError('TABLE_CLOSED',status=500,uncertain=True),False),
        (BLUFFPOKER,BluffPokerError('TABLE_CLOSED',status=404,uncertain=True),False),
        (BLUFFPOKER,BluffPokerError('AUTH_FAILED',status=404),False),
        (EVENBET,BluffPokerError('TABLE_CLOSED',status=404),False),
    ]:
        session=Session.__new__(Session);session.platform=platform
        session.client=SimpleNamespace(tables={},states={42:{'s':[]}},join=AsyncMock(side_effect=error))
        if absent:
            assert asyncio.run(session.adopt(42,{'i':42})) is None
            assert 42 not in session.client.tables and 42 not in session.client.states
        else:
            with pytest.raises(BluffPokerError) as caught:asyncio.run(session.adopt(42,{'i':42}))
            assert caught.value is error
            assert 42 in session.client.tables and 42 in session.client.states
