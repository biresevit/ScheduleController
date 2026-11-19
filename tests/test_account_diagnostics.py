import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from schedule_controller.account_diagnostics import balance_native,sample,enabled,before_logout,capture,wallets
from poker_controller.sites import BLUFFPOKER,EVENBET


def test_only_verified_native_balance():
    assert balance_native(BLUFFPOKER,{'available':12000})==12000
    for value in (-1,True,1.5,'12000',None,2**64):
        assert balance_native(BLUFFPOKER,{'available':value}) is None
    assert balance_native(EVENBET,{'available':12000}) is None


def test_evenbet_vendor_balances_update_keeps_wallets_separate():
    reply={'t':'BalancesUpdate','cashInfoList':[{'mt':1,'ma':12000,'bma':99},{'mt':2,'ma':42,'bma':0}],'private':'discard'}
    assert wallets(EVENBET,reply)==[{'money_type':1,'available_native':12000},{'money_type':2,'available_native':42}]
    assert wallets(EVENBET,{'cashInfoList':[{'mt':1,'ma':True}]}) is None
    assert wallets(EVENBET,{'cashInfoList':[{'mt':1,'ma':12},{'mt':1,'ma':15}]}) is None
    assert enabled(SimpleNamespace(diagnostic_enabled=True))
    assert wallets(EVENBET,{'cashInfoList':[{'mt':'1','ma':'12000'}]})==[{'money_type':1,'available_native':12000}]
    assert wallets(EVENBET,{'cashInfoList':[{'mt':'1','ma':'12000.00'}]})==[{'money_type':1,'available_native':12000}]


def test_bluff_diagnostic_read_never_reauthenticates():
    hub=SimpleNamespace(access='existing-private-token',failed=None,
                        call=AsyncMock(return_value={'available':12000}),authed=AsyncMock(),refresh=AsyncMock())
    session=SimpleNamespace(platform=BLUFFPOKER,client=SimpleNamespace(hub=hub))
    assert asyncio.run(sample(session))==(12000,'balance_observed')
    hub.authed.assert_not_called();hub.refresh.assert_not_called()
    hub.call.assert_awaited_once_with('GET','/player/account/summary',bearer=hub.access,topic='diagnostic-summary')
    hub.failed='CREDENTIAL_EXPIRED'
    assert asyncio.run(sample(session))==(None,'authentication_unavailable')
    assert hub.call.await_count==1


def test_evenbet_uses_existing_socket_and_unknown_shape():
    request=AsyncMock(return_value={'unknown':123})
    session=SimpleNamespace(platform=EVENBET,client=SimpleNamespace(connection=SimpleNamespace(request=request)))
    assert asyncio.run(sample(session))==(None,'balance_shape_unverified')
    request.assert_awaited_once_with('GetBalance')


def test_opt_in(monkeypatch):
    monkeypatch.delenv('POKERFILL_ACCOUNT_DIAGNOSTICS',raising=False)
    assert not enabled()
    monkeypatch.setenv('POKERFILL_ACCOUNT_DIAGNOSTICS','1')
    assert enabled()


def test_final_balance_is_saved_after_monitor_stops(monkeypatch):
    monkeypatch.setenv('POKERFILL_ACCOUNT_DIAGNOSTICS','1')
    order=[]
    async def fake_sample(session):
        assert session.diagnostic_monitor.done()
        order.append('balance');return 1234,'balance_observed'
    monkeypatch.setattr('schedule_controller.account_diagnostics.sample',fake_sample)
    def publish(*args):order.append(args)
    async def run():
        monitor=asyncio.create_task(asyncio.sleep(999))
        session=SimpleNamespace(repo=SimpleNamespace(publish_account_diagnostic=publish),
            policy={'BotAccountId':7},diagnostic_monitor=monitor)
        await before_logout(session)
    asyncio.run(run())
    assert order==['balance',(7,1234,'balance_observed','logout')]


def test_balance_failure_does_not_block_logout(monkeypatch):
    monkeypatch.setenv('POKERFILL_ACCOUNT_DIAGNOSTICS','1')
    monkeypatch.setattr('schedule_controller.account_diagnostics.sample',AsyncMock(side_effect=TimeoutError))
    writes=[]
    session=SimpleNamespace(repo=SimpleNamespace(publish_account_diagnostic=lambda *args:writes.append(args)),policy={'BotAccountId':7})
    asyncio.run(before_logout(session))
    assert writes==[(7,None,'balance_read_failed','logout')]


def test_idle_close_samples_before_socket_closes(monkeypatch):
    from schedule_controller.pilot import Pilot
    order=[]
    async def final(session):order.append('snapshot')
    async def close():order.append('logout')
    monkeypatch.setattr('schedule_controller.account_diagnostics.before_logout',final)
    async def run():
        pilot=Pilot.__new__(Pilot)
        pilot.entries={'closed':{'account':7,'status':'closed'}}
        pilot.client_lock=asyncio.Lock()
        pilot.clients={7:SimpleNamespace(close=close)}
        pilot.policies={7:{'Username':'test'}}
        assert await pilot.close_session_if_idle(7)
    asyncio.run(run())
    assert order==['snapshot','logout']


def test_active_account_does_not_take_logout_snapshot(monkeypatch):
    from schedule_controller.pilot import Pilot
    final=AsyncMock()
    monkeypatch.setattr('schedule_controller.account_diagnostics.before_logout',final)
    pilot=Pilot.__new__(Pilot)
    pilot.entries={'live':{'account':7,'status':'seated'}}
    assert not asyncio.run(pilot.close_session_if_idle(7))
    final.assert_not_called()


def test_final_snapshot_waits_for_inflight_periodic_sql(monkeypatch):
    import threading
    from schedule_controller.account_diagnostics import monitor
    monkeypatch.setenv('POKERFILL_ACCOUNT_DIAGNOSTICS','1')
    monkeypatch.setattr('schedule_controller.account_diagnostics.sample',AsyncMock(return_value=(100,'balance_observed')))
    started,release=threading.Event(),threading.Event()
    writes=[]
    def publish(aid,amount,outcome,source):
        if source=='periodic':
            started.set()
            assert release.wait(3)
        writes.append(source)
    async def run():
        session=SimpleNamespace(repo=SimpleNamespace(publish_account_diagnostic=publish),policy={'BotAccountId':7})
        session.diagnostic_monitor=asyncio.create_task(monitor(session))
        assert await asyncio.to_thread(started.wait,3)
        final=asyncio.create_task(before_logout(session))
        await asyncio.sleep(.02)
        assert not final.done()
        release.set()
        await final
    try:asyncio.run(run())
    finally:release.set()
    assert writes==['periodic','logout']


@pytest.mark.parametrize('amount,outcome,source',[
    (-1,'balance_observed','logout'),(True,'balance_observed','logout'),
    (123,'balance_read_failed','logout'),(None,'balance_observed','logout'),
    (123,'balance_observed','untrusted')])
def test_invalid_snapshot_never_reaches_sql(amount,outcome,source):
    from schedule_controller.pilot_repository import Repository
    repo=Repository('epoch',1,1)
    with pytest.raises(ValueError):repo.publish_account_diagnostic(7,amount,outcome,source)
