import asyncio
import logging
from poker_controller.manual import action_to_play
from poker_controller.state import turn_key

log=logging.getLogger(__name__)


class ManualBridge:
    def __init__(self,repo,aid,tid):self.repo=repo;self.aid=aid;self.tid=tid
    async def offer(self,key):return await asyncio.to_thread(self.repo.manual_offer,self.aid,self.tid,key)
    async def claim_auto(self,key):return await asyncio.to_thread(self.repo.claim_table_action,self.aid,self.tid,key)
    async def play(self,bot,state,hero,key,command):
        try:action,funds=action_to_play(command,state,hero,bot.mapper)
        except ValueError as e:
            await asyncio.to_thread(self.repo.finish_manual_action,command['ActionId'],'failed',str(e));return
        if not await asyncio.to_thread(self.repo.claim_table_action,self.aid,self.tid,key,command):return
        latest=bot.client.states.get(self.tid,{})
        if bot.client.hero_seat(latest)!=hero or bot.mapper.turn_key(latest,hero)!=key:
            await asyncio.to_thread(self.repo.finish_manual_action,command['ActionId'],'expired','The turn changed before submission.');return
        bot.submitted.add(key)
        try:
            await bot.client.play(self.tid,action,funds)
            bot.actions+=1
            await asyncio.to_thread(self.repo.finish_manual_action,command['ActionId'],'sent','The table acknowledged the action.')
        except Exception:
            log.exception('Manual action confirmation unavailable: %s',command['ActionId'])
            await asyncio.to_thread(self.repo.finish_manual_action,command['ActionId'],'uncertain','No confirmation received. Check the table; this action will not be retried.')


async def run_manual(pilot):
    watched=set()
    pending={}
    def dispatch(aid,tid,session,bot):
        key=(aid,tid)
        if key in pending and not pending[key].done():return
        async def process():
            try:await bot.process(session.client.states.get(tid,{}))
            except asyncio.CancelledError:raise
            except Exception:log.exception('Manual turn synchronization failed for %s/%s',aid,tid)
        pending[key]=asyncio.create_task(process())
    while True:
        try:
            rows=await asyncio.to_thread(pilot.repo.manual_watches)
            active=set()
            for r in rows:
                aid=r['BotAccountId'];tid=int(r['TableId']);active.add((aid,tid))
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
        await asyncio.sleep(.5)
