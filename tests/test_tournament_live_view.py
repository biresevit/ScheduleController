import asyncio
from types import SimpleNamespace

from schedule_controller.pilot import Pilot


def live_pilot():
    p=Pilot.__new__(Pilot)
    state={'gi':123,'m':{'ai':0,'di':1,'r':0},'d':{'c':'','p':300},'s':[
        {'i':10,'dn':'opponent','c':9900,'b':100,'d':'-1;-1','f':0},
        {'i':20,'dn':'andrew','c':9800,'b':200,'d':'8;9','f':0}]}
    client=SimpleNamespace(states={42:state,43:state},state_versions={42:1,43:1},
        tables={42:{'n':'SNG table 1'},43:{'n':'SNG table 2'}},hero_seat=lambda value:1)
    p.clients={1:SimpleNamespace(client=client,bots={},policy={'Username':'andrew'},
                                tourneys=SimpleNamespace(active={123:{'table':42}}))}
    captured=[]
    p.repo=SimpleNamespace(
        live_view_requests=lambda:[{'RequestId':'sng','BotAccountId':1,'TableId':'T123'},
                                  {'RequestId':'ring','BotAccountId':1,'TableId':'42'}],
        manual_snapshot=lambda *args:{'enabled':True},
        publish_live_view=lambda rid,hand,payload:captured.append((rid,payload)) or True)
    return p,captured


def run_steps(p,monkeypatch,steps):
    import schedule_controller.pilot as module
    clock=[100.0]
    monkeypatch.setattr(module.time,'monotonic',lambda:clock[0])
    steps=iter(steps)
    async def sleep(_):
        try:next(steps)()
        except StopIteration:raise asyncio.CancelledError
    monkeypatch.setattr(module.asyncio,'sleep',sleep)
    try:asyncio.run(p.run_live_views())
    except asyncio.CancelledError:pass


def test_real_live_loop_publishes_sng_and_ring_without_integer_conversion_error(monkeypatch):
    p,captured=live_pilot()
    run_steps(p,monkeypatch,[])
    assert [rid for rid,_ in captured]==['sng','ring']
    payload=captured[0][1]
    assert payload['table_id']=='42' and payload['table_name']=='SNG table 1'
    assert payload['tournament'] is True
    assert not payload['manual']['supported'] and not payload['manual']['enabled']
    assert payload['seats'][0]['cards']==[] and payload['seats'][1]['cards']


def test_sng_table_move_with_equal_version_publishes_before_heartbeat(monkeypatch):
    p,captured=live_pilot()
    def move():p.clients[1].tourneys.active[123]['table']=43
    run_steps(p,monkeypatch,[move])
    assert [payload['table_id'] for rid,payload in captured if rid=='sng']==['42','43']
    assert [rid for rid,_ in captured].count('ring')==1


def test_waiting_tournament_does_not_block_ring_and_starts_publishing_when_seated(monkeypatch):
    p,captured=live_pilot();p.clients[1].tourneys.active={}
    def seat():p.clients[1].tourneys.active[123]={'table':43}
    run_steps(p,monkeypatch,[seat])
    assert [rid for rid,_ in captured]==['ring','sng']
    assert captured[-1][1]['table_id']=='43'


def test_bad_request_does_not_block_other_live_views(monkeypatch):
    p,captured=live_pilot()
    p.repo.live_view_requests=lambda:[{'RequestId':'bad','BotAccountId':1,'TableId':'not-a-table'},
                                     {'RequestId':'ring','BotAccountId':1,'TableId':'42'}]
    run_steps(p,monkeypatch,[])
    assert [rid for rid,_ in captured]==['ring']
