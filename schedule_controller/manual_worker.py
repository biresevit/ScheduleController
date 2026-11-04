import asyncio
import logging
import time
from poker_controller.manual import action_to_play
from poker_controller.sites import table_key
from poker_controller.state import turn_key
from poker_controller.transport import ProtocolError

log=logging.getLogger(__name__)

# How long the table may take to show a sent action before it counts as not applied.
APPLY_SECONDS=5


class ManualBridge:
    def __init__(self,repo,aid,tid):self.repo=repo;self.aid=aid;self.tid=tid
    async def offer(self,key):return await asyncio.to_thread(self.repo.manual_offer,self.aid,self.tid,key)
    async def claim_auto(self,key):return await asyncio.to_thread(self.repo.claim_table_action,self.aid,self.tid,key)
    async def manual_enabled(self):return await asyncio.to_thread(self.repo.manual_enabled,self.aid,self.tid)

    async def finish(self,command,status,detail):
        await asyncio.to_thread(self.repo.finish_manual_action,command['ActionId'],status,detail)

    async def refuse(self,bot,command,key,detail):
        """The table did not take the action: report why and let the operator choose again."""
        bot.submitted.discard(key)
        await asyncio.to_thread(self.repo.release_turn,self.aid,self.tid,key)
        await self.finish(command,'failed',detail)

    async def applied(self,bot,hero,key):
        deadline=time.monotonic()+APPLY_SECONDS
        while time.monotonic()<deadline:
            latest=bot.client.states.get(self.tid,{})
            if bot.client.hero_seat(latest)!=hero or bot.mapper.turn_key(latest,hero)!=key:return True
            await asyncio.sleep(.1)
        return False

    async def play(self,bot,state,hero,key,command):
        if getattr(bot,'table_detached',False):
            await self.finish(command,'expired','The tournament table changed. Open a new live view.');return
        try:action,funds=action_to_play(command,state,hero,bot.mapper)
        except ValueError as e:
            await self.finish(command,'failed',str(e));return
        if not await asyncio.to_thread(self.repo.claim_table_action,self.aid,self.tid,key,command):return
        latest=bot.client.states.get(self.tid,{})
        if getattr(bot,'table_detached',False) or bot.client.hero_seat(latest)!=hero or bot.mapper.turn_key(latest,hero)!=key:
            await self.finish(command,'expired','The turn changed before submission.');return
        bot.submitted.add(key)
        try:
            await bot.client.play(self.tid,action,funds)
        except ProtocolError as e:
            # An explicit refusal changed nothing at the table, so the turn can be retried.
            text=str(e.response.get('text') or '').strip() or 'no reason given'
            log.warning('Table refused manual action %s: %s',command['ActionId'],text)
            await self.refuse(bot,command,key,f'The table refused this action: {text}. Choose again.');return
        except Exception:
            log.exception('Manual action confirmation unavailable: %s',command['ActionId'])
            await self.finish(command,'uncertain','No confirmation received. Check the table; this action will not be retried.');return
        bot.actions+=1
        if await self.applied(bot,hero,key):
            await self.finish(command,'sent','The table acknowledged the action.')
        else:
            log.warning('Table ignored manual action %s for %ss',command['ActionId'],APPLY_SECONDS)
            await self.refuse(bot,command,key,'The table did not apply this action, so nothing was played. Choose again.')

    async def table_commands(self,bot):
        """Run queued sit-out / I'm-back commands; they do not depend on whose turn it is."""
        while True:
            command=await asyncio.to_thread(self.repo.claim_table_command,self.aid,self.tid)
            if not command:return
            if getattr(bot,'table_detached',False):
                await self.finish(command,'expired','The tournament table changed. Open a new live view.');return
            try:
                if command['Action']=='sit_out':
                    await bot.client.sit_out_next_hand(self.tid)
                    bot.sit_out_requested=True
                    detail='The hero will sit out after this hand.'
                else:
                    await bot.client.sit_back(self.tid)
                    bot.sit_out_requested=False
                    detail="I'm back was sent to the table."
                await self.finish(command,'sent',detail)
            except ProtocolError as e:
                text=str(e.response.get('text') or '').strip() or 'no reason given'
                await self.finish(command,'failed',f'The table refused: {text}')
            except Exception:
                log.exception('Manual table command failed: %s',command['ActionId'])
                await self.finish(command,'uncertain','No confirmation received. Check the table.')


async def run_manual(pilot):
    watched=set()
    pending={}
    def dispatch(aid,tid,session,bot):
        key=(aid,tid)
        if key in pending and not pending[key].done():return
        async def process():
            try:
                if bot.control and hasattr(bot.control,'table_commands'):
                    async with bot.action_lock:await bot.control.table_commands(bot)
                await bot.process(session.client.states.get(tid,{}))
            except asyncio.CancelledError:raise
            except Exception:log.exception('Manual turn synchronization failed for %s/%s',aid,tid)
        pending[key]=asyncio.create_task(process())
    while True:
        try:
            rows=await asyncio.to_thread(pilot.repo.manual_watches)
            active=set()
            for r in rows:
                aid=r['BotAccountId'];tid=table_key(r['TableId']);active.add((aid,tid))
                session=pilot.clients.get(aid);bot=session.bots.get(tid) if session else None
                if not bot or not session.ready.is_set():continue
                # Resume automatic play on switch-off, including when no new
                # GameState event arrives. Manual commands use the latest state.
                if r['Pending'] or not r['Enabled']:
                    if session.client.states.get(tid):dispatch(aid,tid,session,bot)
            for aid,tid in watched-active:
                session=pilot.clients.get(aid);bot=session.bots.get(tid) if session else None
                if bot and session.ready.is_set():dispatch(aid,tid,session,bot)
            watched=active
            pending={k:t for k,t in pending.items() if not t.done()}
        except asyncio.CancelledError:
            for task in pending.values():task.cancel()
            await asyncio.gather(*pending.values(),return_exceptions=True)
            raise
        except Exception:log.exception('Manual table synchronization failed')
        await asyncio.sleep(.25)
