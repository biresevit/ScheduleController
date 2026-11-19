"""Optional wallet observability using existing owned connections only.

Never refreshes credentials, retries a login, or persists a raw response.
EvenBet wire fields follow the vendor's BalancesUpdate cashInfoList converter.
"""
import asyncio
import os
import logging
import re
from poker_controller.sites import BLUFFPOKER,EVENBET
log=logging.getLogger(__name__)


def wire_integer(value):
    if type(value) is int:return value
    if isinstance(value,str) and re.fullmatch(r'-?[0-9]{1,19}(?:\.0+)?',value):return int(value.split('.')[0])
    return None


def enabled(session=None):
    return os.getenv('POKERFILL_ACCOUNT_DIAGNOSTICS') == '1' or bool(getattr(session,'diagnostic_enabled',False))


def wallets(platform,response):
    """Only monetary fields; never store a raw response or infer a fiat mapping."""
    if not isinstance(response,dict):return None
    if platform==BLUFFPOKER:
        amount=balance_native(platform,response)
        return [{'money_type':None,'available_native':amount}] if amount is not None else None
    rows=response.get('cashInfoList') if platform==EVENBET else None
    if not isinstance(rows,list) or not 1<=len(rows)<=16:return None
    result=[];seen=set()
    for row in rows:
        if not isinstance(row,dict):return None
        kind,amount=wire_integer(row.get('mt')),wire_integer(row.get('ma'))
        if type(kind) is not int or kind in seen or type(amount) is not int or not 0<=amount<=2**63-1:return None
        seen.add(kind);result.append({'money_type':kind,'available_native':amount})
    return result


def balance_native(platform, response):
    if platform != BLUFFPOKER or not isinstance(response, dict):
        return None
    # Preserve the account-summary native integer units, as used by
    # BluffPokerTournaments.affordable. Do not infer a fiat conversion.
    value = response.get('available')
    return value if type(value) is int and 0 <= value <= 2**63-1 else None


async def sample(session):
    client = session.client
    if session.platform == BLUFFPOKER:
        hub = client.hub
        if not hub.access or hub.failed:
            return None, 'authentication_unavailable'
        # Direct existing bearer: authed()/balance() can refresh or re-login.
        response = await asyncio.wait_for(hub.call('GET', '/player/account/summary', bearer=hub.access, topic='diagnostic-summary'), 8)
    else:
        # Existing WebSocket request has no login/reconnect path.
        response = await asyncio.wait_for(client.connection.request('GetBalance'), 8)
    session.diagnostic_wallets=wallets(session.platform,response)
    if session.diagnostic_wallets is None:
        # Types/field names only: never write response values, tokens or account identities.
        def types(value,depth=0):
            if depth<3 and isinstance(value,dict):return {str(k)[:40]:types(v,depth+1) for k,v in list(value.items())[:20]}
            if depth<3 and isinstance(value,list):return [types(v,depth+1) for v in value[:2]]
            return type(value).__name__
        shape=types(response)
        log.warning('Wallet response shape unverified (platform %s): %s',session.platform,shape)
    amount = session.diagnostic_wallets[0]['available_native'] if session.diagnostic_wallets and len(session.diagnostic_wallets)==1 else None
    return amount, 'balance_observed' if session.diagnostic_wallets else 'balance_shape_unverified'


async def capture(session, source='periodic'):
    session.diagnostic_wallets=None
    try:
        amount, outcome = await sample(session)
    except asyncio.CancelledError:
        raise
    except Exception:
        amount, outcome = None, 'balance_read_failed'
    kwargs={'wallets':session.diagnostic_wallets} if session.diagnostic_wallets is not None else {}
    write_task=asyncio.create_task(asyncio.to_thread(session.repo.publish_account_diagnostic,
                                session.policy['BotAccountId'], amount, outcome, source,**kwargs))
    try:
        # Cancelling an asyncio wrapper cannot stop a SQL thread. Wait for that
        # write before the final sample, so an old periodic write cannot win last.
        await asyncio.shield(write_task)
    except asyncio.CancelledError:
        await asyncio.gather(write_task,return_exceptions=True)
        raise
    except Exception as error:
        log.warning('Wallet snapshot persistence failed (%s)',type(error).__name__)


async def before_logout(session):
    """Stop periodic samples so they cannot race the final, post-table snapshot."""
    if not enabled(session) or not getattr(session,'repo',None):return
    monitor_task=getattr(session,'diagnostic_monitor',None)
    if monitor_task:
        monitor_task.cancel()
        await asyncio.gather(monitor_task,return_exceptions=True)
    await capture(session,'logout')


async def monitor(session):
    while True:
        await capture(session)
        await asyncio.sleep(300)
