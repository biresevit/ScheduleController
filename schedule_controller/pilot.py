"""Bounded Nenad/Evenbet live pilot using the shared planner on one PC.

The SQL task journal, leases and telemetry are real. Scheduling/execution run in
this one process for the pilot; no claim of a distributed Azure worker is made.
"""
from __future__ import annotations
import argparse
import asyncio
from collections import Counter
import copy
from datetime import datetime,timedelta,timezone
from decimal import Decimal
import heapq
import json
import logging
import os
from pathlib import Path
import random
import tempfile
import time
import uuid

from poker_controller.bot import Bot
from poker_controller.sites import BLUFFPOKER,EVENBET,create_client,platform_id,table_key
from poker_controller.decision import PDecision
from poker_controller.state import amount_scale,cards
from poker_controller.transport import ProtocolError
from .planner import Account,Entry,Machine,Settings,Table,plan
from .rules import Band,Window,target_at,choose_buy_in
from .pilot_repository import Repository,NAMES
from .join_failures import LIMIT_REASON,LIMIT_DESCRIPTION,failure_description
from .tournament_worker import TOPICS as TOURNAMENT_TOPICS,TournamentPlay,lobby_rows as tournament_lobby_rows,remember_buy_ins,tournament_id
from .bluffpoker_tournaments import BluffPokerTournaments,lobby_rows as bluffpoker_tournament_rows

log=logging.getLogger(__name__)
# Audited ordinary NLH/PLO4 demo tables. No fast-fold, double-board, bomb-pot,
# all-in-or-fold, anonymous or currency-unknown tables are selected in the pilot.
TABLE_IDS={128197714,128016534,128048285,128048284,128053412,
           113891676,88548202,128053413,128094710}

def now(): return datetime.now(timezone.utc)
def ring_table(entry):
    """The table key of a ring entry (EvenBet: int, BluffPoker: UUID text); None for a tournament entry ('T' + id)."""
    table=str(entry['table'])
    return None if table.startswith('T') else table_key(table)
def guid(): return str(uuid.uuid4())
def aware(value):
    if value is None: return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


class WorkerLock:
    """An operating-system lock per machine and site, held while a cloud worker runs on this PC.

    Windows releases it as soon as the holding process exits, also when it crashes, so whoever acquires it knows
    the previous worker of this machine and site is gone."""

    def __init__(self,machine_id,site_id,folder=None):
        self.path=Path(folder or tempfile.gettempdir())/f'pokersuite-worker-{machine_id}-{site_id}.lock'
        self.handle=None

    def acquire(self):
        try:
            self.path.parent.mkdir(parents=True,exist_ok=True)
            handle=open(self.path,'a+b')
        except OSError:
            return False
        try:
            if os.name=='nt':
                import msvcrt
                handle.seek(0)
                msvcrt.locking(handle.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError:
            handle.close()
            return False
        self.handle=handle
        return True

    def release(self):
        if self.handle:
            try:self.handle.close()
            finally:self.handle=None


# Accounts whose seats are proven at the same time after a restart.
RECONCILE_CONCURRENCY = 6


def rate_limited(exc):
    """A poker site's rate-limit answer (HTTP 429, RATE_LIMITED / RATE_LIMIT_EXCEEDED)."""
    return getattr(exc,'status',None)==429 or 'RATE_LIMIT' in str(getattr(exc,'code','') or exc)


def observed_ring_table(table):
    """Ring tables published to the lobby (Live Tables): NLH, PLO4 and PLO5, no-limit or pot-limit. The scheduler
    decides separately which of them bots join (planner: NLH and PLO4 only)."""
    return table.get('g') in (72,79,88) and table.get('p') in (80,85)


class Session:
    def __init__(self,policy,decision,rng=None,repo=None,site=None):
        self.policy=policy
        self.repo=repo
        self.platform=platform_id((site or {}).get('Platform'))
        self.endpoint=(site or {}).get('EndpointUrl')
        # BluffPoker's site-wide client values, stored with the site; the PC then needs no bluffpoker.json
        self.client_settings={'api_key':(site or {}).get('ClientApiKey'),'origin':(site or {}).get('ClientOrigin')}
        self.client=self.new_client()
        self.decision=decision
        self.rng=rng or random.Random()
        self.bots={}
        self.queues={}
        self.workers={}
        self.failures={}
        self.pump=None
        self.observed={}
        self.rit_responses=set()
        self.rit_tasks=set()
        self.bust_tasks=set()
        # Pilot hook: on_bust_result(tid, amount) after a rebuy, (tid, None) when the seat should be left.
        self.on_bust_result=None
        # Pilot hook: on_break_change(tables, True) when the account sits out for a break, (tables, False) after.
        self.on_break_change=None
        self.break_until=None
        self.breaker=None
        self.ready=asyncio.Event()
        self.ready.set()
        self.attach_observer(self.client,self.observed)
        if self.platform==EVENBET:self.tourneys=TournamentPlay(self,log)
        elif self.platform==BLUFFPOKER:self.tourneys=BluffPokerTournaments(self,log)

    def new_client(self):
        """A logged-out client of this account on the site's platform."""
        p=self.policy
        return create_client(self.platform,p['Username'],endpoint_url=None if self.platform==EVENBET else self.endpoint,
                             email=p.get('LoginEmail'),password=p.get('LoginPassword'),**getattr(self,'client_settings',{}))

    def attach_observer(self,client,observed):
        # Reconnect candidates must not refresh timestamps on an older client's
        # cached seats. Each connection owns its own observation clock.
        def receive(message):
            if message.get('t')=='GameState':
                state=message.get('gameState',message.get('GameState',{}))
                if state.get('ti'):observed[state['ti']]=now()
        client.connection.listeners.append(receive)

    def stamp(self,message):
        if message.get('t')=='GameState':
            s=message.get('gameState',message.get('GameState',{}))
            if s.get('ti'): self.observed[s['ti']]=now()

    async def open(self,purpose='play task'):
        await self.client.open()
        self.pump=asyncio.create_task(self.receive())
        if purpose=='play task':self.breaker=asyncio.create_task(self.run_breaks())
        log.info('Login confirmed for %s: %s',purpose,self.policy['Username'])

    def next_break_after(self):
        p=self.policy;low=int(p.get('BreakAfterMinSeconds') or 1200);high=max(low,int(p.get('BreakAfterMaxSeconds') or 3600))
        return self.rng.uniform(low,high)

    def break_length(self):
        p=self.policy;low=int(p.get('BreakMinSeconds') or 10);high=max(low,int(p.get('BreakMaxSeconds') or 300))
        return low+(high-low)*self.rng.betavariate(1,3) # Mostly short breaks, occasionally long ones.

    def playing_tables(self):
        # Tournament tables never take breaks: sitting out there only blinds the stack away.
        return [tid for tid,bot in self.bots.items() if tid in self.queues and tid not in self.failures
                and tid not in getattr(getattr(self,'tourneys',None),'by_table',())
                and not bot.leaving and not bot.busted and not bot.on_break]

    async def run_breaks(self,tick=5):
        """Count active play time; when the sampled break point is reached, take a break on every table."""
        remaining=self.next_break_after()
        while True:
            await asyncio.sleep(tick)
            if not self.ready.is_set() or not self.playing_tables():
                continue # The clock only runs while the account is playing.
            remaining-=tick
            if remaining>0:
                continue
            try:
                await self.take_break()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception('Break failed for %s',self.policy['Username'])
            remaining=self.next_break_after()

    async def take_break(self):
        """Sit out after the current hand on all tables at once, wait, then press I'm back on each.

        One simulated person never plays some tables while away from others. Tables under manual
        control are left to the human; a leave started during the break wins over returning.
        """
        tables=[]
        for tid in self.playing_tables():
            bot=self.bots[tid]
            if bot.control and await bot.control.manual_enabled():
                continue
            tables.append(tid)
        if not tables:
            return
        length=self.break_length()
        try:
            for tid in tables:
                self.bots[tid].on_break=True
                try:await self.client.sit_out_next_hand(tid)
                except Exception as exc:log.warning('Break sit-out not accepted for %s / %s: %s',self.policy['Username'],tid,exc)
            if self.on_break_change:self.on_break_change(tables,True)
            def away(state):
                hero=self.client.hero_seat(state)
                return hero is None or bool(state['s'][hero].get('f',0)&1)
            async def finish_hand(tid):
                try:await self.client.wait_state(tid,away,timeout=180)
                except TimeoutError:log.warning('Hand did not finish before the break for %s / %s',self.policy['Username'],tid)
            await asyncio.gather(*(finish_hand(tid) for tid in tables))
            self.break_until=time.time()+length
            log.info('BREAK %s: away from %s tables for %ss',self.policy['Username'],len(tables),round(length))
            await asyncio.sleep(length)
        except asyncio.CancelledError:
            # Logging out or reconnecting: never press I'm back from a closing session.
            for tid in tables:
                if tid in self.bots:self.bots[tid].on_break=False
            self.break_until=None
            raise
        except Exception:
            log.exception('Break interrupted for %s; returning to the tables',self.policy['Username'])
        back=[]
        for tid in tables:
            bot=self.bots.get(tid)
            if not bot or not bot.on_break:continue
            bot.on_break=False
            if bot.leaving or tid not in self.queues or (bot.control and await bot.control.manual_enabled()):continue
            state=self.client.states.get(tid,{});hero=self.client.hero_seat(state)
            if hero is not None and state['s'][hero].get('f',0)&1:
                try:await self.client.sit_back(tid)
                except Exception as exc:log.warning('I\'m back not accepted after a break for %s / %s: %s',self.policy['Username'],tid,exc)
            back.append(tid)
        self.break_until=None
        if self.on_break_change and back:self.on_break_change(back,False)
        log.info('BREAK over for %s: back on %s tables',self.policy['Username'],len(back))

    async def receive(self):
        while True:
            try:
                event=await self.client.connection.next_event()
                if event.get('t') in TOURNAMENT_TOPICS and hasattr(self,'tourneys'):
                    self.tourneys.handle(event)
                if event.get('t')=='GameState':
                    s=event.get('gameState',event.get('GameState',{})); tid=s.get('ti')
                    if tid in self.queues:
                        q=self.queues[tid]
                        if q.full(): q.get_nowait(); q.task_done()
                        q.put_nowait(s)
                elif event.get('t')=='NotifyFromServer' and event.get('resultCode'):
                    log.warning('Server notification for %s: code %s',self.policy['Username'],event.get('resultCode'))
                elif event.get('t')=='RunItTwiceOffer':
                    tid=int(event.get('tableId') or 0)
                    hand=self.client.states.get(tid,{}).get('gi')
                    key=(tid,hand)
                    if tid in self.queues and key not in self.rit_responses:
                        self.rit_responses.add(key)
                        accept=self.rng.random()<float(self.policy['RunItTwiceChance'])
                        task=asyncio.create_task(self.respond_run_it_twice(tid,accept,self.rng.uniform(0,7)))
                        task.rit_table=tid
                        self.rit_tasks.add(task)
                        task.add_done_callback(self.rit_tasks.discard)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.queues:
                    log.warning('Connection lost for active account %s: %s; reconnecting',
                                self.policy['Username'],type(exc).__name__)
                    self.ready.clear()
                    if await self.reconnect_active():
                        self.ready.set()
                        continue
                    self.failures[-1]=type(exc).__name__
                    self.ready.set()
                    log.error('Active reconnect failed for %s',self.policy['Username'])
                    return
                log.warning('Idle connection lost for %s: %s; reconnecting',
                            self.policy['Username'],type(exc).__name__)
                if not await self.reconnect_idle():
                    self.failures[-1]=type(exc).__name__
                    return

    async def reconnect_idle(self):
        for delay in (1,3,10):
            try:
                await self.client.close()
                self.client=self.new_client()
                self.observed={}
                self.attach_observer(self.client,self.observed)
                await asyncio.sleep(delay)
                await self.client.open()
                log.info('Idle login reconnected: %s',self.policy['Username'])
                return True
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning('Idle reconnect attempt failed for %s: %s',
                            self.policy['Username'],type(exc).__name__)
        return False

    async def reconnect_active(self):
        """Reconnect surviving seats; a departed table cannot reject the rest."""
        old=self.client;table_info=copy.deepcopy(old.tables)
        # Tournament tables are re-opened by TournamentPlay.resume (select + claim the seat with the
        # tournament's entry index); the ring resume below would only leave the bot watching.
        tourneys=getattr(self,'tourneys',None);tournament_tables=set(getattr(tourneys,'by_table',{}))
        table_ids=[tid for tid in self.queues if tid not in tournament_tables]
        for delay in (1,3,10):
            candidate=None
            try:
                await asyncio.sleep(delay)
                candidate=self.new_client()
                observed={};self.attach_observer(candidate,observed)
                await candidate.open();present=[]
                for tid in table_ids:
                    if tid in table_info:candidate.tables[tid]=table_info[tid]
                    await candidate.join(tid)
                    hero=candidate.hero_seat(candidate.states[tid])
                    if hero is not None:
                        try:
                            if candidate.states[tid]['s'][hero].get('f',0)&1:await candidate.return_from_away(tid)
                            await candidate.player_command(tid,10)
                        except ProtocolError:
                            log.warning('Resume command not accepted for %s / %s; retaining observed seat for reconciliation',self.policy['Username'],tid)
                        present.append(tid)
                tasks=list(self.workers.values())
                for task in tasks:task.cancel()
                await asyncio.gather(*tasks,return_exceptions=True)
                self.client=candidate;self.observed=observed;self.workers={};self.queues={}
                for tid in table_ids:
                    if tid not in present:self.bots.pop(tid,None)
                    self.failures.pop(tid,None)
                self.failures.pop(-1,None)
                for tid in present:
                    self.bots[tid].client=candidate;self.queues[tid]=asyncio.Queue(maxsize=1)
                    self.queues[tid].put_nowait(copy.deepcopy(candidate.states[tid]))
                    self.workers[tid]=asyncio.create_task(self.worker(tid))
                for tid in tournament_tables:self.bots.pop(tid,None)
                if tournament_tables:
                    tourneys.by_table.clear()
                    for state in tourneys.active.values():state['table']=None
                    tourneys.spawn(tourneys.resume(),'resume after reconnect')
                await old.close()
                log.info('Active login reconnected: %s (%s surviving, %s absent tables)',self.policy['Username'],len(present),len(table_ids)-len(present))
                return True
            except asyncio.CancelledError:
                if candidate:await candidate.close()
                raise
            except Exception as exc:
                log.warning('Active reconnect attempt failed for %s: %s',self.policy['Username'],type(exc).__name__)
                if candidate:await candidate.close()
        return False

    async def respond_run_it_twice(self,tid,accept,delay):
        try:
            await asyncio.sleep(delay)
            if tid not in self.queues:
                return
            await self.client.player_command(tid,17 if accept else 18)
            log.info('Run It Twice %s: %s / %s', 'accepted' if accept else 'declined',
                     self.policy['Username'],tid)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # The offer can expire while the human-like response delay elapses.
            # It is optional and must not invalidate a successfully seated entry.
            log.warning('Run It Twice response expired for %s / %s: %s',
                        self.policy['Username'],tid,type(exc).__name__)

    async def worker(self,tid):
        q=self.queues[tid]
        try:
            while True:
                state=await q.get()
                try: await self.bots[tid].process(state)
                finally: q.task_done()
        except asyncio.CancelledError: raise
        except Exception as exc:
            log.exception('Table worker failed: %s / %s',self.policy['Username'],tid)
            self.failures[tid]=type(exc).__name__

    async def add(self,tid,info,rng):
        await self.ready.wait()
        if self.failures:
            raise RuntimeError('Account connection is unhealthy')
        self.client.tables[tid]=info
        await self.client.join(tid)
        if self.client.hero_seat(self.client.states[tid]) is not None:
            raise RuntimeError('Unexpected existing seat requires reconciliation')
        amount=await self.buy_in_amount(tid,rng)
        post=rng.random()<float(self.policy['PostBbChance'])
        await self.client.sit(tid,str(amount),wait_big_blind=not post)
        self.bots[tid]=self.new_bot(tid)
        self.queues[tid]=asyncio.Queue(maxsize=1)
        self.workers[tid]=asyncio.create_task(self.worker(tid))
        self.queues[tid].put_nowait(copy.deepcopy(self.client.states[tid]))
        return amount

    async def buy_in_amount(self,tid,rng):
        limits=await self.client.buy_in_range(tid)
        minimum=Decimal(limits['minAmount'])/100; maximum=Decimal(limits['maxAmount'])/100
        bb=Decimal(self.client.tables[tid]['bb'])/100
        weights=json.loads(self.policy['BuyInPolicyJson'])
        try:
            return choose_buy_in(weights,bb,minimum,maximum,maximum,rng)
        except ValueError as error:
            if maximum<minimum or bb<=0:
                raise ValueError(f'{error}. Server buy-in range: {minimum}–{maximum} chips; big blind: {bb}.') from error
        # No configured amount is legal, typically when rejoining soon after leaving: the server
        # then demands the stack the account left with. Buy in for the legal amount closest to
        # the account's most likely choice (exactly that stack when the range is one value).
        preferred=max((w,k) for k,w in weights.items() if k!='irregular' and w)[1] if any(w for k,w in weights.items() if k!='irregular') else 100
        amount=min(maximum,max(minimum,(Decimal(preferred)*bb).quantize(Decimal('0.01'))))
        log.info('Buy-in for %s / %s: no configured amount fits %s–%s; using %s',self.policy['Username'],tid,minimum,maximum,amount)
        return amount

    def new_bot(self,tid):
        return Bot(self.client,self.decision,tid,hand_history=True,control=self.table_control(tid),on_bust=self.on_table_bust,
                   on_show_offer=self.show_offer)

    def on_table_bust(self,tid):
        if tid in getattr(getattr(self,'tourneys',None),'by_table',()):self.tourneys.on_bust(tid)
        else:self.bust(tid)

    async def attach_table(self,tid):
        """Start the bot for a tournament table the server seated us at (no buy-in, no sit-down)."""
        if tid in self.workers:return
        self.bots[tid]=self.new_bot(tid)
        self.queues[tid]=asyncio.Queue(maxsize=1)
        self.workers[tid]=asyncio.create_task(self.worker(tid))
        state=self.client.states.get(tid)
        if state:self.queues[tid].put_nowait(copy.deepcopy(state))

    async def detach_table(self,tid):
        """Stop a tournament table's bot without leaving (moved to another table, or out of the tournament)."""
        bot=self.bots.get(tid)
        if bot:bot.table_detached=True
        repo=getattr(self,'repo',None)
        if repo:
            try:await asyncio.to_thread(repo.end_live_table_views,self.policy['BotAccountId'],tid)
            except Exception:log.exception('Could not close live windows for detached tournament table %s',tid)
        worker=self.workers.pop(tid,None)
        if worker:worker.cancel()
        self.queues.pop(tid,None);self.bots.pop(tid,None)

    def show_offer(self,tid,state,hero):
        # Short table responses share the run-it-twice task set: cancelled when the table or session closes.
        task=asyncio.create_task(self.maybe_show(tid,copy.deepcopy(state),hero))
        task.rit_table=tid
        self.rit_tasks.add(task)
        task.add_done_callback(self.rit_tasks.discard)

    async def maybe_show(self,tid,state,hero):
        """After an uncontested win, show a qualifying hand with the account's show-cards chance."""
        from poker_controller.handrank import show_reason
        try:
            bot=self.bots.get(tid)
            if not bot or (bot.control and await bot.control.manual_enabled()):
                return
            game=state.get('gt',self.client.tables.get(tid,{}).get('g'))
            reason=show_reason(game,cards(state['s'][hero].get('d')),cards(state.get('d',{}).get('c')))
            chance=self.policy.get('ShowCardsChance')
            chance=.05 if chance is None else float(chance)
            if not reason or self.rng.random()>=chance:
                return
            await asyncio.sleep(self.rng.uniform(.6,2.5))
            latest=self.client.states.get(tid,{})
            if latest.get('gi')!=state.get('gi') or latest.get('m',{}).get('ai')!=hero or 10 not in latest.get('aa',[]):
                return # The offer expired while "thinking".
            await self.client.play(tid,10,0)
            log.info('SHOW %s table=%s hand=%s (%s)',self.policy['Username'],tid,state.get('gi'),reason)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning('Show cards failed for %s / %s: %s',self.policy['Username'],tid,exc)

    def bust(self,tid):
        task=asyncio.create_task(self.rebuy_or_leave(tid))
        task.bust_table=tid
        self.bust_tasks.add(task)
        task.add_done_callback(self.bust_tasks.discard)

    async def rebuy_or_leave(self,tid):
        """After losing the whole stack, buy back in with the account's ring rebuy chance, else leave."""
        try:
            await asyncio.sleep(self.rng.uniform(3,8)) # A person reads the result before the dialog.
            await self.ready.wait()
            bot=self.bots.get(tid);state=self.client.states.get(tid,{});hero=self.client.hero_seat(state)
            if not bot or bot.leaving or hero is None or tid in self.failures:return
            if state['s'][hero].get('c',0) or state['s'][hero].get('b',0):
                bot.busted=False;return # The final snapshot credited chips after all.
            chance=self.policy.get('RebuyRingChance')
            chance=.8 if chance is None else float(chance)
            if self.rng.random()<chance:
                try:
                    amount=await self.buy_in_amount(tid,self.rng)
                    post=self.rng.random()<float(self.policy['PostBbChance'])
                    await self.client.rebuy(tid,str(amount),wait_big_blind=not post)
                    log.info('REBUY %s table=%s amount=%s',self.policy['Username'],tid,amount)
                    if self.on_bust_result:self.on_bust_result(tid,amount)
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.warning('Rebuy failed for %s / %s: %s; leaving the table',self.policy['Username'],tid,exc)
            else:
                log.info('BUSTED %s table=%s; leaving without a rebuy (chance %s)',self.policy['Username'],tid,chance)
            if self.on_bust_result:self.on_bust_result(tid,None)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception('Bust handling failed for %s / %s',self.policy['Username'],tid)

    def table_control(self,tid):
        if not self.repo:return None
        from .manual_worker import ManualBridge
        return ManualBridge(self.repo,self.policy['BotAccountId'],tid)

    async def adopt(self,tid,info):
        """Attach workers to a seat proven to exist after a controller restart."""
        self.client.tables[tid]=info
        await self.client.join(tid)
        state=self.client.states.get(tid,{})
        hero=self.client.hero_seat(state)
        if hero is None:
            return None
        try:
            if state['s'][hero].get('f',0)&1:await self.client.return_from_away(tid)
            await self.client.player_command(tid,10)
        except ProtocolError:
            log.warning('Resume command not accepted for %s / %s; retaining observed seat for reconciliation',self.policy['Username'],tid)
        state=self.client.states.get(tid,state)
        hero=self.client.hero_seat(state)
        if hero is None:return None
        self.bots[tid]=self.new_bot(tid)
        self.queues[tid]=asyncio.Queue(maxsize=1)
        self.workers[tid]=asyncio.create_task(self.worker(tid))
        self.queues[tid].put_nowait(copy.deepcopy(state))
        return 'sitting_out' if state['s'][hero].get('f',0)&1 else 'seated'

    async def remove(self,tid,finish=True):
        await self.ready.wait()
        for task in list(self.rit_tasks):
            if getattr(task,'rit_table',None)==tid:
                task.cancel()
        for task in list(self.bust_tasks):
            if getattr(task,'bust_table',None)==tid and task is not asyncio.current_task():
                task.cancel()
        if tid in self.bots:
            self.bots[tid].leaving=True # An intentional break: the bot must not press I'm back.
        state=self.client.states.get(tid,{});hero=self.client.hero_seat(state)
        broke=hero is not None and not state['s'][hero].get('c',0) and not state['s'][hero].get('b',0)
        if finish and not broke and tid in self.bots and tid not in self.failures:
            await self.client.player_command(tid,47) # Away after this hand.
            def finished(s):
                hero=self.client.hero_seat(s)
                return hero is None or s.get('ts')==3 or bool(s['s'][hero].get('f',0)&1)
            await self.client.wait_state(tid,finished,timeout=180)
            if tid in self.queues:
                try: await asyncio.wait_for(self.queues[tid].join(),20)
                except TimeoutError: log.warning('History flush timed out for %s / %s',self.policy['Username'],tid)
        await self.client.leave(tid)
        if tid in self.workers:
            self.workers.pop(tid).cancel()
        self.queues.pop(tid,None)

    async def close(self):
        if getattr(self,'tourneys',None):await self.tourneys.close()
        for t in self.workers.values(): t.cancel()
        for t in self.rit_tasks: t.cancel()
        for t in self.bust_tasks: t.cancel()
        extra=[t for t in (self.pump,getattr(self,'breaker',None)) if t]
        for t in extra: t.cancel()
        await asyncio.gather(*self.workers.values(),*self.rit_tasks,*self.bust_tasks,*extra,return_exceptions=True)
        await self.client.close()


class Pilot:
    def __init__(self,args):
        self.args=args;self.epoch=guid();self.repo=Repository(self.epoch,getattr(args,'site_id',1),getattr(args,'machine_id',1));self.rng=random.Random(args.seed)
        self.clients={};self.entries={};self.sessions=[];self.accounting={};self.intervals=[];self.sequence=0
        self.info={};self.tables=[];self.seats={};self.pending=[];self.jobs={};self.errors=[]
        self.next_join=self.next_leave=self.last_plan=None;self.saved=None;self.started=False
        self.decision=PDecision(os.getenv('PDECISION_URL','http://127.0.0.1:5005'),api_key=os.getenv('PDECISION_API_KEY','')) # one PDecision serves every poker site of the PC
        self.output=Path(args.output);self.output.mkdir(parents=True,exist_ok=True)
        self.publish_lock=asyncio.Lock()
        self.client_lock=asyncio.Lock()
        self.execution_locks={}

    async def configuration(self):
        self.config=await asyncio.to_thread(self.repo.configuration)
        self.policies={a['BotAccountId']:a for a in self.config['accounts']}
        self.bands=[Band(b['Tier'],b['MinBigBlind'],b['MaxBigBlind'],b['TargetBasisPoints'],bool(b['UpperInclusive'])) for b in self.config['bands']]
        s=self.config['site']
        self.settings=Settings(1,self.bands,max_joins=s['MaxJoinsPerTick'],max_leaves=s['MaxLeavesPerTick'],
            min_gap=s['MinJoinGapSeconds'],max_gap=s['MaxJoinGapSeconds'])
        for aid,client in self.clients.items(): client.policy=self.policies[aid]

    @property
    def platform(self):
        return platform_id(self.config['site'].get('Platform'))

    async def session_for(self,aid):
        """Authenticate an account only when a play command is being executed.

        One lock per account: logins of different accounts run at the same time (restart reconciliation of 40+
        accounts must finish while BluffPoker still holds their seats), the same account never logs in twice."""
        async with self.client_lock:
            existing=self.clients.get(aid)
            if existing:
                return existing
            lock=self.__dict__.setdefault('account_locks',{}).setdefault(aid,asyncio.Lock())
        async with lock:
            existing=self.clients.get(aid)
            if existing:
                return existing
            session=Session(self.policies[aid],self.decision,self.rng,repo=self.repo,site=self.config['site'])
            session.on_bust_result=lambda tid,amount,aid=aid:self.bust_result(aid,tid,amount)
            session.on_break_change=lambda tables,away,aid=aid:self.break_change(aid,tables,away)
            if hasattr(session,'tourneys'):session.tourneys.on_event=self.tournament_event
            await session.open('play task')
            self.clients[aid]=session
            if hasattr(session,'tourneys'):
                # Play every tournament table EvenBet already seated this account at, also one whose
                # registration the database no longer holds (e.g. cut off by an immediate update).
                session.tourneys.spawn(session.tourneys.resume(),'resume at login')
            return session

    def bust_result(self,aid,tid,amount):
        """Count a rebuy into the entry's buy-in, or leave a seat that has no chips left."""
        e=next((x for x in self.entries.values() if x['account']==aid and ring_table(x)==tid
                and x['status'] in ('seated','sitting_out')),None)
        if not e:return
        if amount is not None:e['buy_in']=Decimal(str(e.get('buy_in') or 0))+Decimal(str(amount))
        else:self.start_leave(e)

    def break_change(self,aid,tables,away):
        """A break keeps the seats (they still count toward the target) but shows them as sitting out."""
        for e in self.entries.values():
            if e['account']==aid and ring_table(e) in tables and e['status'] in ('seated','sitting_out'):
                e['status']='sitting_out' if away else 'seated'

    def tournament_event(self,aid,key,event,details):
        """Update a tournament entry: 'seated' (playing, with the current table) or 'finished'."""
        e=next((x for x in self.entries.values() if x['account']==aid and str(x['table'])==key
                and x['status'] not in ('closed','failed')),None)
        if not e:
            log.warning('Tournament %s event %s for %s has no open entry',key,event,self.policies[aid]['Username']);return
        if event=='seated':
            e.update(status='seated',seated=e.get('seated') or now(),confirmed=now(),play_table=str(details.get('table')))
            log.info('SEATED %s tournament=%s table=%s',self.policies[aid]['Username'],key,details.get('table'))
        elif event=='finished':
            self.accrue();e.update(status='closed',closed=now(),confirmed=now());self.accrue()
            log.info('FINISHED %s tournament=%s place=%s',self.policies[aid]['Username'],key,details.get('place'))
        elif event in ('reentry','rebuy','addon'):
            # The entry stays open: a re-entry or rebuy keeps the bot in the same tournament.
            e.update(confirmed=now())
            log.info('%s %s tournament=%s %s',event.upper(),self.policies[aid]['Username'],key,details)
        async def persist():
            try:
                await self.publish()
                if event=='finished':await self.close_session_if_idle(aid)
            except Exception:log.exception('Could not persist tournament %s for %s',key,self.policies[aid]['Username'])
        asyncio.create_task(persist())

    async def close_session_if_idle(self,aid):
        """Log out after the account's final planned or active entry closes."""
        if any(e['account']==aid and e['status'] not in ('closed','failed') and not e.get('parked') for e in self.entries.values()):
            return False
        async with self.client_lock:
            session=self.clients.pop(aid,None)
            if not session:
                return False
            await session.close()
        log.info('Logout confirmed after final table: %s',self.policies[aid]['Username'])
        return True

    async def park_tournament_registration(self,e):
        await asyncio.to_thread(self.repo.park_registered_entry,e['id'])
        e.update(parked=True,session=None,status='joining',play_table=None)
        client=self.clients.get(e['account'])
        if client:client.tourneys.park_registration(tournament_id(e['table']))
        log.info('PARKED future MTT registration %s / %s; no playing session',self.policies[e['account']]['Username'],e['table'])

    def close_empty_registration_return(self,aid,session_id,cmd,reason):
        if not (cmd.get('payload') or {}).get('new_session'):return
        if any(e['account']==aid and not e.get('parked') and e['status'] not in ('closed','failed') for e in self.entries.values()):return
        for session in self.sessions:
            if str(session['id'])==str(session_id) and not session.get('closed'):
                session.update(closed=max(now(),session['start']),reason=reason)

    async def refresh(self):
        scan_started=time.monotonic()
        observer=None
        temporary=None
        last_error=None
        # A single account socket must never be a controller-wide dependency.
        candidates=sorted(self.clients.values(),key=lambda c: bool(c.failures or c.client.connection.failure))
        # A disabled PC logs no account in just to scan the lobby; seated accounts still read their own tables.
        if not any(not c.failures and not c.client.connection.failure for c in candidates) and                 await asyncio.to_thread(getattr(self.repo,'machine_enabled',lambda:True)):
            scan_account=await asyncio.to_thread(self.repo.claim_observer)
            if scan_account is not None and scan_account not in self.policies:
                await asyncio.to_thread(self.repo.release_observer,scan_account)
                await self.configuration()
                scan_account=await asyncio.to_thread(self.repo.claim_observer)
            if scan_account is not None:
                temporary=Session(self.policies[scan_account],self.decision,self.rng,site=self.config['site'])
                try:await temporary.open('lobby scan')
                except BaseException:
                    await asyncio.to_thread(self.repo.release_observer,scan_account)
                    raise
                candidates.append(temporary)
        for candidate in candidates:
            try:
                await candidate.ready.wait()
                if candidate.failures or candidate.client.connection.failure:
                    continue
                await candidate.client.list_tables()
                lobby=list(candidate.client.tables.values())
                # EvenBet's demo lobby is limited to the audited tables; BluffPoker's lobby lists only plain
                # NLH/PLO ring tables (the client drops tournament and special formats).
                allowed=TABLE_IDS if self.platform==EVENBET else None
                info={t['i']:t for t in lobby if (allowed is None or t['i'] in allowed) and observed_ring_table(t)}
                # Subscribe to every table concurrently instead of one at a time; a lobby
                # scan has nothing to gain from pacing itself like real table play does.
                async def join_table(tid):
                    version=candidate.client.state_versions.get(tid,0)
                    await candidate.client.join(tid)
                    await candidate.client.wait_state(tid,lambda s,tid=tid,version=version: candidate.client.state_versions.get(tid,0)>version)
                await asyncio.gather(*(join_table(tid) for tid in info))
                observer=candidate
                self.info=info
                break
            except Exception as exc:
                last_error=exc
                log.warning('Lobby observer failed for %s: %s',
                            candidate.policy['Username'],type(exc).__name__)
                # Every other account would get the same answer and only prolong the limit (34 failed scans
                # in a row on machine 5, 2026-11-08 06:17).
                if rate_limited(exc):
                    break
        if observer is None:
            if temporary: await temporary.close()
            if temporary:await asyncio.to_thread(self.repo.release_observer,temporary.policy['BotAccountId'])
            if not candidates:
                self.info={};self.tables=[];self.seats={}
                return
            # The bots keep playing: only this lobby snapshot is missing. Stopping the site here dropped every
            # seat (BluffPoker keeps a disconnected seat ~29 s) because of a lobby rate limit (machine 5, 06:17).
            log.warning('Lobby scan skipped this round, no observer could list the lobby: %s',
                        type(last_error).__name__ if last_error else 'no answer')
            return
        observer_states=copy.deepcopy(observer.client.states)
        observer_observed=dict(observer.observed)
        # SNGs and MTTs are published as lobby rows too; a failed listing only skips them this round.
        try:observer_tournaments=await observer.client.list_tournaments() if hasattr(observer,'tourneys') else None
        except Exception as exc:
            observer_tournaments=None
            log.warning('Tournament listing failed for %s: %s',observer.policy['Username'],type(exc).__name__)
        if temporary:
            await temporary.close()
            await asyncio.to_thread(self.repo.release_observer,temporary.policy['BotAccountId'])
            log.info('Logout confirmed after lobby scan: %s',temporary.policy['Username'])
        stamp=now(); native={c.client.player_id:aid for aid,c in self.clients.items()}
        # Our own accounts are split across machines; the observer that happens
        # to run this scan only has this machine's accounts in self.clients.
        # Fall back to matching on username so a seat held by an account logged
        # in from another machine still resolves instead of being counted as an
        # unknown occupant on every scan.
        by_username={p['Username'].casefold():aid for aid,p in getattr(self,'policies',{}).items()}
        self.tables=[];self.seats={}
        for tid,info in self.info.items():
            state=observer_states.get(tid,{})
            stamp_state=observer_observed.get(tid)
            if not stamp_state or len(state.get('s',[]))!=int(info['mp']): continue
            live=[(i,s) for i,s in enumerate(state.get('s',[])) if s and not s.get('f',0)&128]
            self.tables.append({'id':str(tid),'name':info['n'],'game':{72:'NLH',79:'PLO4',88:'PLO5'}[info['g']],
                'table_type':'RING',
                'bb':Decimal(info['bb'])/100,'minimum':Decimal(info.get('mn',0))/100,'maximum':Decimal(info.get('mx',0))/100,
                'seats':info['mp'],'occupied':len(live),'open':True,'observed':stamp,'snapshot':stamp_state})
            self.seats[str(tid)]=[{'seat':i,'native_id':str(s.get('i','')),'name':s.get('dn') or s.get('n',''),
                'account':native.get(s.get('i')) or by_username.get((s.get('dn') or s.get('n','')).casefold()),
                'stack':Decimal(s.get('c',0))/100,'bet':Decimal(s.get('b',0))/100,
                'in_game':bool(s.get('d')),'away':bool(s.get('f',0)&1),'hand':str(state.get('gi','')),'observed':stamp_state} for i,s in live]
        if observer_tournaments is not None:
            rows=bluffpoker_tournament_rows if self.platform==BLUFFPOKER else tournament_lobby_rows
            self.tables.extend(rows(observer_tournaments,stamp))
            remember_buy_ins(self.platform,self.tables)
        took=time.monotonic()-scan_started
        (log.warning if took>10 else log.debug)('Lobby scan of %s tables took %.1f s',len(self.tables),took)
        await self.reconcile_observed_entries()

    async def reconcile_observed_entries(self):
        """Close only seats absent in a complete fresh snapshot newer than the journal."""
        stamp=now();tables={t['id']:t for t in self.tables};closed_accounts=set()
        for entry in list(self.entries.values()):
            if entry['status'] not in ('seated','sitting_out','draining','uncertain'):continue
            if any(not task.done() for key,task in self.jobs.items() if key==entry['id'] or (isinstance(key,tuple) and entry['id'] in key)):continue
            table=tables.get(entry['table']);players=self.seats.get(entry['table'])
            observed=table.get('snapshot') if table else None
            if not observed or observed<stamp-timedelta(seconds=60) or players is None or len(players)!=table['occupied']:continue
            policy=self.policies[entry['account']];connection=self.clients.get(entry['account'])
            native=str(connection.client.player_id) if connection else None
            present=any(p.get('account')==entry['account'] or (p.get('name') or '').casefold()==policy['Username'].casefold() or (native and p.get('native_id')==native) for p in players)
            if present:
                if entry['status']!='uncertain' and (not entry.get('confirmed') or observed>entry['confirmed']):entry['confirmed']=observed
                continue
            last=entry.get('confirmed') or entry.get('seated') or entry['created']
            if observed<=last:continue
            self.accrue();entry.update(status='closed',closed=observed)
            for command in entry.get('commands',[]):
                if command['status'] in ('pending','claimed','uncertain'):
                    command.update(status='succeeded' if command['kind']=='leave' else 'cancelled',result='reconciled_absent',result_text='A fresh complete table snapshot confirms the bot is no longer seated.')
            closed_accounts.add(entry['account'])
            transport=getattr(getattr(connection,'client',None),'connection',None)
            failure=getattr(transport,'failure',None);session_ready=getattr(connection,'ready',None)
            log.info('Live reconciliation confirmed absent: %s / %s; observed=%s previous_confirmation=%s connection_failed=%s session_ready=%s',
                policy['Username'],entry['table'],observed,last,
                type(failure).__name__ if failure else None,session_ready.is_set() if session_ready else False)
        for account in closed_accounts:
            if not getattr(self,'cloud',False) and not any(e['account']==account and e['status'] not in ('closed','failed') for e in self.entries.values()):
                for session in self.sessions:
                    if session['account']==account and not session.get('closed'):session.update(closed=max(session['start'],stamp),reason='reconciled_absent')
            try:await self.close_session_if_idle(account)
            except Exception:log.exception('Could not close reconciled idle account')
        if closed_accounts:self.accrue()

    def accrue(self):
        # AccountInterval stores milliseconds (datetime2(3)) and requires EndUtc>StartUtc: two accruals within the
        # same millisecond made an empty interval that failed the whole publish and restarted the site (2026-11-07).
        stamp=now();stamp=stamp.replace(microsecond=stamp.microsecond//1000*1000)
        counts=Counter(e['account'] for e in self.entries.values() if e['status'] in ('seated','sitting_out','draining'))
        for aid,(start,count) in list(self.accounting.items()):
            if stamp>start and count:
                self.sequence+=1
                self.intervals.append({'id':guid(),'account':aid,'sequence':self.sequence,'start':start,'end':stamp,'entries':count})
        self.accounting={aid:(stamp,n) for aid,n in counts.items()}

    async def publish(self,ready=True):
        async with self.publish_lock:
            self.accrue()
            spans=copy.deepcopy(self.intervals)
            # Closed entries and sessions stay in memory all day, and the journal writes each one row by row: on a
            # PC far from the database, re-sending a day's worth took ~25 s per publish, so the lobby reached the
            # database already stale and Live tables and the scheduler often saw no fresh tables. Active ones are
            # always sent (their leases are renewed); closed ones only until the same state has been written once.
            sent=self.__dict__.setdefault('published_closed',{})
            def unsent(kind,item,closed):
                return not closed or sent.get((kind,item['id']))!=json.dumps(item,default=str,sort_keys=True)
            entries=[e for e in self.entries.values() if unsent('entry',e,e['status'] in ('closed','failed'))]
            sessions=[s for s in self.sessions if unsent('session',s,bool(s.get('closed')))]
            entries=copy.deepcopy(entries);sessions=copy.deepcopy(sessions)
            # The lobby goes out once per scan (refresh() builds a new list), not again with every join or leave step.
            fresh_lobby=self.tables is not self.__dict__.get('published_tables')
            tables,seats=(copy.deepcopy(self.tables),copy.deepcopy(self.seats)) if fresh_lobby else ([],{})
            started=time.monotonic()
            await asyncio.to_thread(self.repo.publish,tables,seats,
                entries,sessions,spans,ready=ready,
                next_join=self.next_join,next_leave=self.next_leave,last_plan=self.last_plan)
            took=time.monotonic()-started
            if fresh_lobby:self.published_tables=self.tables
            # The scheduler treats tables older than ~45 s and heartbeats older than 60 s as stale.
            (log.warning if took>10 else log.debug)('Published %s lobby tables, %s entries, %s sessions and %s intervals in %.1f s',
                len(tables),len(entries),len(sessions),len(spans),took)
            for kind,items,closed in (('entry',entries,lambda e:e['status'] in ('closed','failed')),('session',sessions,lambda s:bool(s.get('closed')))):
                for item in items:
                    if closed(item):sent[(kind,item['id'])]=json.dumps(item,default=str,sort_keys=True)
            done={s['id'] for s in spans};self.intervals=[s for s in self.intervals if s['id'] not in done]
            summary={'controller_version':getattr(self.args,'controller_version',None),'at':now().isoformat(),'epoch':self.epoch,'target':self.args.target,
                'entries':Counter(e['status'] for e in self.entries.values()),
                'accounts':{p['Username']:sum(e['account']==a and e['status'] in ('seated','sitting_out','draining') for e in self.entries.values()) for a,p in self.policies.items()},
                'actions':sum(b.actions for c in self.clients.values() for b in c.bots.values()),'errors':self.errors,
                'end':self.end.isoformat(),'sessions':self.sessions}
            tmp=self.output/'status.tmp';tmp.write_text(json.dumps(summary,default=str,indent=2),encoding='utf-8');tmp.replace(self.output/'status.json')

    async def publish_live_views(self):
        requests=await asyncio.to_thread(self.repo.live_view_requests)
        for request in requests:
            await self.publish_live_view(request)
        return len(requests)

    async def run_live_views(self):
        """Publish live snapshots on their own task, as soon as the hero's table state changes.

        Lobby scans, bulk publishes and command claims in the main worker loop can take
        seconds; sharing that loop left the viewer (and manual control, whose turn controls
        come from these snapshots) up to 20s behind the real table. The 1s heartbeat keeps
        the snapshot fresh and carries manual-control acknowledgements between hands.
        """
        requests=[];next_requests=0.0;published={}
        while True:
            try:
                tick=time.monotonic()
                if tick>=next_requests:
                    requests=await asyncio.to_thread(self.repo.live_view_requests)
                    next_requests=tick+(1 if requests else 3)
                    # Manual control exists only under a live view; the manual loop reads this
                    # instead of polling the database while nobody is watching.
                    self.live_requests_seen=time.monotonic() if requests else 0.0
                    live={str(r['RequestId']) for r in requests}
                    published={k:v for k,v in published.items() if k in live}
                for request in requests:
                    try:
                        rid=str(request['RequestId']);session=self.clients.get(request['BotAccountId'])
                        tid=self.live_table_id(request)
                        # Tournament IDs name a registration, not an integer table.
                        # Include the playing table so moves publish immediately even
                        # when both tables happen to have the same state version.
                        version=(tid,session.client.state_versions.get(tid)) if session else None
                        last=published.get(rid)
                        if last and last[0]==version and tick-last[1]<1:
                            continue
                        if await self.publish_live_view(request):
                            published[rid]=(version,tick)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        log.exception('Live view publishing failed for request %s / table %s',request.get('RequestId'),request.get('TableId'))
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception('Live view publishing failed')
            await asyncio.sleep(.1 if requests else 1)

    def live_table_id(self,request):
        key=str(request['TableId'])
        session=self.clients.get(request['BotAccountId'])
        if key.startswith('T'):
            # The request names the tournament; follow the table the bot plays now (also after a move).
            tid=((getattr(session,'tourneys',None) and session.tourneys.active.get(tournament_id(key))) or {}).get('table')
            return table_key(tid) if tid else None
        return table_key(key)

    async def publish_live_view(self,request):
        aid=request['BotAccountId'];key=str(request['TableId'])
        session=self.clients.get(aid)
        tournament=key.startswith('T')
        tid=self.live_table_id(request)
        bound=request.get('BoundTableId')
        if tournament and bound and str(tid)!=str(bound):
            await asyncio.to_thread(self.repo.end_live_table_views,aid,table_key(bound))
            return False
        state=session.client.states.get(tid) if session else None
        if not state:
            return False
        hero=session.client.hero_seat(state)
        if hero is None:
            return False
        table_info=session.client.tables.get(tid,{})
        # The request also identifies tournaments before optional table metadata arrives.
        scale=1 if tournament else amount_scale(table_info)
        payload={'table_id':str(tid),'table_name':table_info.get('n',str(tid)),
            'game':{72:'NLH',79:'PLO4',88:'PLO5'}.get(table_info.get('g'),'NLH'),
            'hero':session.policy['Username'],'hand_id':str(state.get('gi','')),
            'max_seats':len(state.get('s',[])),
            'amount_unit':'tournament_chips' if scale==1 else 'currency',
            'big_blind':state.get('bbv',table_info.get('bb') or 0)/scale,
            'small_blind':state.get('sbv',table_info.get('s') or 0)/scale,
            'street':{0:'Preflop',1:'Flop',2:'Turn',3:'River'}.get(state.get('m',{}).get('r'),'Complete'),
            'board':cards(state.get('d',{}).get('c')),'pot':state.get('d',{}).get('p',0)/scale,
            'dealer':state.get('m',{}).get('di'),'acting_seat':None,'seats':[]}
        acting_seat=state.get('m',{}).get('ai')
        if isinstance(acting_seat,int) and 0<=acting_seat<len(state.get('s',[])):
            actor=state['s'][acting_seat]
            if actor and not actor.get('f',0)&128:
                payload['acting_seat']=acting_seat
        for index,seat in enumerate(state.get('s',[])):
            if not seat or seat.get('f',0)&128:
                continue
            # Opponents' cards are hidden (-1) until they show them at showdown.
            shown=cards(seat.get('d'))
            payload['seats'].append({'seat':index,'name':seat.get('dn') or seat.get('n',''),
                'stack':seat.get('c',0)/scale,'bet':seat.get('b',0)/scale,'in_game':bool(seat.get('d')),
                'sitting_out':bool(seat.get('f',0)&1),'hero':index==hero,
                'cards':shown if index==hero or all(shown) else []})
        result=getattr(session.client,'results',{}).get(tid)
        if result:
            payload['result']={'hand_id':str(result['hand_id'] or ''),'winners':result['winners']}
        from poker_controller.manual import options,clock
        bot=session.bots.get(tid)
        payload['clock']=clock(state,getattr(session.client,'state_times',{}).get(tid),time.time())
        payload['manual']=options(state,hero,bot.mapper if bot else None)
        payload['manual']['amount_scale']=scale
        payload['manual']['sit_out_pending']=bool(bot and bot.sit_out_requested and not payload['manual']['away'])
        payload['manual']['leaving']=bool(bot and bot.leaving)
        payload['manual']['on_break']=bool(bot and bot.on_break)
        payload['manual']['break_seconds']=max(0,round(session.break_until-time.time())) if bot and bot.on_break and getattr(session,'break_until',None) else None
        payload['manual'].update(await asyncio.to_thread(self.repo.manual_snapshot,aid,tid,payload['manual']['turn_key']))
        if tournament:
            payload['tournament']=True
            payload['manual']['table_bound']=True
        return await asyncio.to_thread(self.repo.publish_live_view,request['RequestId'],state.get('gi'),payload)

    async def schedule(self):
        stamp=now()
        await self.configuration()
        windows=[Window(r['Weekday'],r['StartMinute'],r['EndMinute'],r['TargetEntries']) for r in self.config['windows']]
        target=target_at(windows,stamp,self.config['site']['TimeZone']) if self.config['site']['SchedulingEnabled'] else 0
        accounts=[]
        for aid,p in self.policies.items():
            session=next((s for s in reversed(self.sessions) if s['account']==aid and not s.get('closed')),None)
            previous=next((s for s in reversed(self.sessions) if s['account']==aid and s.get('closed')),None)
            used=p['PlayedSeconds']
            accounts.append(Account(aid,1,p['StakeTier'],p['PlayingFrequency'],int(used),p['MaxPlaytimeMinutes'],
                min(3,p['MaxTables']),1 if session else None,enabled=bool(p['IsActive'] and p['IsEnabled']),
                eligible_after=previous['closed']+timedelta(minutes=p['SessionCooldownMinutes']) if previous else None,
                session_end=session['end'] if session else None,requires_reconciliation=bool(self.clients.get(aid) and self.clients[aid].failures)))
        tables=[Table(t['id'],1,t['bb'],t['seats'],t['occupied'],t['snapshot'],game=t['game'],
            observed_accounts=frozenset(p['account'] for p in self.seats[t['id']] if p['account'])) for t in self.tables]
        entries=[Entry(e['id'],e['account'],1,e['table'],e['status']) for e in self.entries.values()]
        m=self.config['machine']
        result=plan(self.settings,stamp,target,accounts,[Machine(1,1,min(self.args.capacity,m['MaxEntries']),stamp,enabled=bool(m['IsEnabled']))],tables,entries,self.rng,
            next_join=self.next_join,next_leave=self.next_leave,last_planned_at=self.last_plan)
        if 'reconciliation_cooldown' in result.reasons:
            return # A premature timer must not move the cooldown forward again.
        self.next_join,self.next_leave,self.last_plan=result.next_join,result.next_leave,stamp
        for action in result.actions:
            if action.kind=='join':
                e={'id':guid(),'account':action.account,'table':action.table,'status':'reserved','created':stamp,'commands':[]}
                self.entries[e['id']]=e
            else: e=self.entries[action.entry]
            command={'id':guid(),'kind':'join' if action.kind=='join' else 'leave','status':'pending',
                     'due':action.not_before,'version':self.config['site']['ConfigVersion']}
            e['commands'].append(command)
            heapq.heappush(self.pending,(action.not_before.timestamp(),command['id'],e['id'],action.kind))
        log.info('Planner target=%s open=%s planned=%s deficits=%s',target,sum(e['status'] not in ('closed','failed') for e in self.entries.values()),len(result.actions),result.remaining_deficits)
        if not result.actions and target:
            log.info('Planner details: reasons=%s accounts=%s tables=%s machine=%s',result.reasons,
                [(a.id,a.tier,a.enabled,a.requires_reconciliation,a.max_tables) for a in accounts],
                [(t.id,t.game,t.occupied,t.seats,round((stamp-t.observed).total_seconds(),1)) for t in tables],m['IsEnabled'])
        await self.publish()

    async def execute(self,e,cmd,kind):
        if str(e['table']).startswith('T'):
            return await self.execute_tournament(e,cmd,kind)
        c=None;tid=table_key(e['table']);phase='persisting task intent'
        if kind=='leave' and e['status'] in ('closed','failed'):
            # The worker already left on its own, e.g. after a bust without a rebuy.
            cmd.update(status='succeeded',result='already_closed');await self.publish();return
        cmd['status']='claimed'
        e['status']='joining' if kind=='join' else 'draining'
        try:
            await self.publish() # Persist intent before touching the poker server.
            if kind=='join':
                if await self.block_ended_session(e,cmd):return
                if not await asyncio.to_thread(self.repo.authorize_join,cmd['version']):
                    e.update(status='closed',closed=now());cmd.update(status='cancelled',result='configuration_changed')
                    return
                info=self.info.get(tid) # Retain a snapshot: refresh replaces self.info while login awaits.
                phase='checking PDecision readiness'
                await self.decision.require_hand_history(self.platform)
                phase='logging in'
                c=await self.session_for(e['account'])
                if now()>self.end or c.failures: raise RuntimeError('Pilot is ending or account is unhealthy')
                if info is None:
                    phase='refreshing the account lobby'
                    await c.client.list_tables()
                    info=c.client.tables.get(tid)
                if info is None:
                    raise RuntimeError(f'Table {tid} is no longer available in this account\'s lobby')
                phase='joining the table and taking a seat'
                amount=await c.add(tid,info,self.rng)
                self.accrue()
                session=next((s for s in reversed(self.sessions) if s['account']==e['account'] and not s.get('closed')),None)
                if not session:
                    began=now()
                    session={'id':guid(),'account':e['account'],'start':began,'end':max(began+timedelta(seconds=1),min(self.end,began+timedelta(minutes=self.rng.randint(c.policy['SessionMinMinutes'],c.policy['SessionMaxMinutes']))))}
                    self.sessions.append(session)
                e.update(status='seated',seated=now(),confirmed=now(),buy_in=amount,session=session['id'])
                self.accrue()
                log.info('SEATED %s table=%s name=%s buyin=%s',c.policy['Username'],tid,info.get('n',tid),amount)
            else:
                phase='leaving the table'
                c=self.clients.get(e['account'])
                if not c: raise RuntimeError('Playing account is not authenticated')
                await c.remove(tid,finish=tid not in c.failures)
                self.accrue();e.update(status='closed',closed=now(),confirmed=now());self.accrue()
                log.info('LEFT %s table=%s',c.policy['Username'],tid)
            cmd['status']='succeeded'
        except Exception as exc:
            username=self.policies[e['account']]['Username']
            error_detail=failure_description(exc,'Join' if kind=='join' else 'Leave',username,tid,phase)
            self.errors.append({'account':username,'table':tid,'kind':kind,'error':type(exc).__name__,'at':now().isoformat()})
            log.exception('Task failed: %s / %s / %s',kind,username,tid)
            try:
                if c: await c.remove(tid,finish=False)
                self.accrue();e.update(status='failed',closed=now());self.accrue()
                cmd.update(status='failed',result=type(exc).__name__,result_text=error_detail[:1000])
            except Exception:
                e['status']='uncertain';cmd.update(status='uncertain',result='cleanup_unconfirmed')
                cmd['result_text']=(error_detail+' Cleanup could not be confirmed; reconciliation is required.')[:1000]
                if c:c.failures[tid]='cleanup_unconfirmed'
        finally:
            if not getattr(self,'cloud',False):
                for s in self.sessions:
                    if not s.get('closed') and not any(x['account']==s['account'] and x['status'] not in ('closed','failed') for x in self.entries.values()):
                        s.update(closed=now(),reason='all_entries_closed')
            if c:
                try: await self.close_session_if_idle(e['account'])
                except Exception: log.exception('Could not close idle account %s',self.policies[e['account']]['Username'])
            try:
                await self.publish()
                if kind=='join' and cmd['status']=='failed':
                    await self.after_join_failure(e)
            except Exception: log.exception('Could not persist task outcome')

    async def block_ended_session(self,e,cmd):
        if not getattr(self,'cloud',False):return False
        if (cmd.get('payload') or {}).get('registration_only') or (cmd.get('payload') or {}).get('resume_registration'):
            return False
        reason=await asyncio.to_thread(self.repo.join_session_block,e.get('session'))
        if not reason:return False
        e.update(status='closed',closed=now())
        cmd.update(status='cancelled',result=reason,result_text=LIMIT_DESCRIPTION if reason==LIMIT_REASON else 'Session has ended; no new join or registration was attempted.')
        return True

    async def after_join_failure(self,e):
        if not getattr(self,'cloud',False):return
        # The failed outcome is committed before counting it. The per-login execution
        # lock keeps already-claimed tasks from sending a third join in the meantime.
        await asyncio.to_thread(self.repo.enforce_join_failure_limit)
        if await asyncio.to_thread(self.repo.join_session_block,e.get('session'))!=LIMIT_REASON:return
        stamp=now()
        for s in self.sessions:
            if str(s['id'])==str(e.get('session')):s.update(end=max(stamp,s['start']),reason=LIMIT_REASON)
        for other in self.entries.values():
            if str(other.get('session'))!=str(e.get('session')):continue
            if other['status'] in ('reserved','joining'):
                other.update(status='closed',closed=stamp)
                for command in other.get('commands',[]):
                    if command['kind']=='join' and command['status'] in ('pending','claimed'):
                        command.update(status='cancelled',result=LIMIT_REASON,result_text=LIMIT_DESCRIPTION)
            elif other['status'] in ('seated','sitting_out') and ring_table(other) is not None:
                self.start_leave(other)
        await self.close_session_if_idle(e['account'])
        await self.publish()
        log.warning('Session %s for %s stopped after two failed joins/registrations',e.get('session'),self.policies[e['account']]['Username'])

    async def execute_tournament(self,e,cmd,kind):
        """Register for an SNG/MTT. The entry stays 'joining' until the tournament seats the bot,
        then 'seated'; it closes when the bot's tournament ends. Tournaments are never left."""
        username=self.policies[e['account']]['Username']
        if kind!='join':
            if e['status'] in ('closed','failed'):
                cmd.update(status='succeeded',result='already_closed');await self.publish();return
            if not (cmd.get('payload') or {}).get('unregister'):
                # Scheduler departures never apply to tournaments: a registered bot plays to the end.
                cmd.update(status='succeeded',result='tournament_kept');await self.publish();return
            return await self.unregister_tournament(e,cmd,username)
        cmd['status']='claimed';e['status']='joining';phase='persisting task intent'
        return_session=e.get('session')
        try:
            await self.publish()
            if await self.block_ended_session(e,cmd):return
            returning=(cmd.get('payload') or {}).get('resume_registration')
            allowed=await asyncio.to_thread(self.repo.authorize_join,cmd['version'],True) if returning else await asyncio.to_thread(self.repo.authorize_join,cmd['version'])
            if not allowed:
                if returning:await self.park_tournament_registration(e)
                else:e.update(status='closed',closed=now())
                if returning:self.close_empty_registration_return(e['account'],return_session,cmd,'registration_return_cancelled')
                cmd.update(status='cancelled',result='configuration_changed');return
            phase='checking PDecision readiness'
            await self.decision.require_hand_history(self.platform)
            phase='logging in'
            c=await self.session_for(e['account'])
            if c.failures:raise RuntimeError('Account connection is unhealthy')
            row=next((t for t in self.tables if str(t['id'])==str(e['table'])),{})
            fmt=(cmd.get('payload') or {}).get('format') or row.get('table_type') or 'SNG'
            phase='registering for the tournament'
            payload=cmd.get('payload') or {}
            if payload.get('resume_registration'):
                if not await c.tourneys.resume_registration(tournament_id(e['table']),fmt):
                    e.update(status='closed',closed=now(),parked=False)
                    cmd.update(status='succeeded',result='registration_gone')
                    self.close_empty_registration_return(e['account'],return_session,cmd,'registration_gone')
                    return
            else:
                from .mtt_sessions import waits_for_mtt
                only=fmt=='MTT' and (payload.get('registration_only') or waits_for_mtt(row,now()))
                if only:await c.tourneys.register(tournament_id(e['table']),fmt,registration_only=True)
                else:await c.tourneys.register(tournament_id(e['table']),fmt)
            session=next((s for s in reversed(self.sessions) if s['account']==e['account'] and not s.get('closed')),None)
            e.update(confirmed=now(),session=e.get('session') or (session and session['id']))
            cmd['status']='succeeded'
            from .mtt_sessions import waits_for_mtt
            if fmt=='MTT' and (payload.get('registration_only') or waits_for_mtt(row,now())):
                await self.park_tournament_registration(e)
            log.info('REGISTERED %s %s %s',username,fmt,e['table'])
        except Exception as exc:
            self.errors.append({'account':username,'table':e['table'],'kind':'register','error':type(exc).__name__,'at':now().isoformat()})
            log.exception('Tournament registration failed: %s / %s',username,e['table'])
            if (cmd.get('payload') or {}).get('resume_registration'):
                await self.park_tournament_registration(e)
                self.close_empty_registration_return(e['account'],return_session,cmd,'registration_return_failed')
            else:e.update(status='failed',closed=now())
            cmd.update(status='failed',result=type(exc).__name__,result_text=failure_description(exc,'Registration',username,e['table'],phase))
            try:await self.close_session_if_idle(e['account'])
            except Exception:log.exception('Could not close idle account %s',username)
        finally:
            try:
                await self.publish()
                if e.get('parked') or e['status']=='closed':await self.close_session_if_idle(e['account'])
                if cmd['status']=='failed':await self.after_join_failure(e)
            except Exception:log.exception('Could not persist tournament registration')

    async def execute_serialized(self,e,cmd,kind):
        """Serialize poker-server mutations per login while allowing accounts in parallel."""
        lock=self.execution_locks.setdefault(e['account'],asyncio.Lock())
        async with lock:
            await self.execute(e,cmd,kind)

    async def unregister_tournament(self,e,cmd,username):
        """Operator Unregister task: cancel an SNG/MTT registration before the tournament seats the bot."""
        cmd['status']='claimed'
        try:
            await self.publish()
            if e['status']!='joining':
                raise ValueError('The tournament has already started for this bot; it plays to the end')
            c=self.clients.get(e['account']) or await self.session_for(e['account'])
            await c.tourneys.unregister(tournament_id(e['table']))
            self.accrue();e.update(status='closed',closed=now(),confirmed=now());self.accrue()
            cmd.update(status='succeeded',result='unregistered')
            log.info('UNREGISTERED %s %s',username,e['table'])
        except Exception as exc:
            self.errors.append({'account':username,'table':e['table'],'kind':'unregister','error':type(exc).__name__,'at':now().isoformat()})
            log.exception('Tournament unregistration failed: %s / %s',username,e['table'])
            cmd.update(status='failed',result=type(exc).__name__,result_text=str(exc)[:1000] if isinstance(exc,ValueError) else None)
        finally:
            try:await self.publish()
            except Exception:log.exception('Could not persist tournament unregistration')

    def accept_claim(self,row):
        """Materialize one persisted Azure command in the local execution journal."""
        eid=str(row['EntryId']);cid=str(row['CommandId'])
        e=self.entries.get(eid)
        if not e:
            e={'id':eid,'account':row['BotAccountId'],'table':str(row['TableId']),
               'status':row['EntryStatus'],'created':aware(row['CreatedUtc']),'commands':[],
               'session':str(row['SessionId']) if row['SessionId'] else None}
            self.entries[eid]=e
        if row['SessionId'] and not any(str(s['id'])==str(row['SessionId']) for s in self.sessions):
            self.sessions.append({'id':str(row['SessionId']),'account':row['BotAccountId'],
                'start':aware(row['StartedUtc']),'end':aware(row['PlannedEndUtc'])})
        try:payload=json.loads(row.get('PayloadJson') or '{}')
        except ValueError:payload={}
        if payload.get('resume_registration'):
            e.update(parked=False,session=str(row['SessionId']),status=row['EntryStatus'])
        cmd={'id':cid,'kind':row['Kind'],'status':'claimed','due':aware(row['NotBeforeUtc']),
             'version':row['ConfigVersion'],'payload':payload if isinstance(payload,dict) else {}}
        e['commands'].append(cmd)
        self.jobs[eid]=asyncio.create_task(self.execute_serialized(e,cmd,row['Kind']))

    async def run_cloud_worker(self):
        """Execute commands planned by Azure; no scheduling is performed locally."""
        # Azure owns session lifetimes here: a session outlives its tables and the scheduler
        # closes it at its planned end. The worker must not end sessions when tables close.
        self.cloud=True
        await self.configuration()
        await self.decision.require_hand_history(self.platform)
        self.end=now()+timedelta(days=3650) if not self.args.seconds else now()+timedelta(seconds=self.args.seconds)
        chat_task=manual_task=live_task=None
        try:
            # Holding the PC's worker lock proves the worker behind a fresh heartbeat has exited: a crashed worker
            # used to block its replacement for 90 s, and BluffPoker gives up a disconnected seat after ~29 s.
            self.worker_lock=WorkerLock(getattr(self.args,'machine_id',None),getattr(self.args,'site_id',None))
            gone=self.worker_lock.acquire()
            recovery=await asyncio.to_thread(self.repo.start_cloud_worker,gone) if gone else                 await asyncio.to_thread(self.repo.start_cloud_worker)
            await self.reconcile_cloud_entries(recovery)
            await asyncio.to_thread(self.repo.finish_cloud_worker_reconciliation)
            self.started=True
            self.args.ready=True  # read by run_sites: a run that got ready restarts quickly after a failure
            log.info('Cloud worker ready after reconciling %s prior entries; entry capacity is enforced by the Azure scheduler',len(recovery))
            await self.refresh();await self.publish()
            from .chat_worker import run_chat
            chat_task=asyncio.create_task(run_chat(self))
            from .manual_worker import run_manual
            manual_task=asyncio.create_task(run_manual(self))
            live_task=asyncio.create_task(self.run_live_views())
            next_refresh=time.monotonic()+self.config['site']['LobbyRefreshSeconds']
            next_claim=next_telemetry=0.0
            while (not self.args.seconds or now()<self.end) and not self.stopping():
                tick=time.monotonic()
                if tick>=next_refresh:
                    await self.refresh();await self.publish()
                    next_refresh=tick+self.config['site']['LobbyRefreshSeconds']
                if tick>=next_claim:
                    claimed=await asyncio.to_thread(self.repo.claim_due,10)
                    for row in claimed:
                        self.accept_claim(row)
                    next_claim=tick+1
                if tick>=next_telemetry and getattr(self.args,'telemetry_provider',None):
                    try:
                        metrics=await asyncio.to_thread(self.args.telemetry_provider)
                        await asyncio.to_thread(self.repo.publish_machine_telemetry,metrics)
                    except Exception:
                        log.exception('Could not publish machine resource telemetry')
                    next_telemetry=tick+300
                await asyncio.sleep(.25)
        finally:
            if live_task:
                live_task.cancel()
                await asyncio.gather(live_task,return_exceptions=True)
            if manual_task:
                manual_task.cancel()
                await asyncio.gather(manual_task,return_exceptions=True)
            if chat_task:
                chat_task.cancel()
                await asyncio.gather(chat_task,return_exceptions=True)
            if self.started:
                try: await asyncio.to_thread(self.repo.stop_cloud_worker)
                except Exception: log.exception('Could not mark cloud worker offline')
                await asyncio.gather(*self.jobs.values(),return_exceptions=True)
                if getattr(self.args,'leave_on_exit',False):
                    log.info('Explicit leave-on-exit requested; draining active poker entries')
                    for e in list(self.entries.values()):
                        if e['status'] in ('seated','sitting_out','joining','draining'):
                            self.start_leave(e)
                    while any(not job.done() for job in self.jobs.values()):
                        await asyncio.sleep(1)
                    try: await self.refresh();await self.publish(ready=False)
                    except Exception: log.exception('Final cloud-worker telemetry failed')
                else:
                    log.info('Preserving active poker seats for restart reconciliation')
            for c in list(self.clients.values()): await c.close()
            self.clients.clear()
            await self.decision.close()
            log.info('Cloud worker stopped. Status: %s',self.output/'status.json')
            if getattr(self,'worker_lock',None):self.worker_lock.release()

    def stopping(self):
        # A PC playing several poker sites runs one Pilot per site: the worker's stop marker stops them all, and the
        # worker stops one site's Pilot when the site is taken off the PC.
        event=getattr(self.args,'stop_event',None)
        return bool(event and event.is_set()) or Path(getattr(self.args,'stop_file',None) or self.output/'stop').exists()

    async def reconcile_cloud_entries(self,rows):
        """Prove old-epoch seats against the poker site before making this worker ready.

        BluffPoker keeps a disconnected seat about 29 s: accounts are reconciled in parallel, and the lobby is listed
        once for all of them (one listing per account rate-limited the hub and arrived too late for the later seats)."""
        grouped={}
        for row in rows:
            grouped.setdefault(row['BotAccountId'],[]).append(row)
        shared={'lobby':None};lobby_lock=asyncio.Lock();slots=asyncio.Semaphore(RECONCILE_CONCURRENCY)
        async def lobby_for(session):
            async with lobby_lock:
                if shared['lobby'] is None:
                    await session.client.list_tables()
                    shared['lobby']={table_key(t['i']):dict(t) for t in session.client.tables.values()}
            return shared['lobby']
        async def one(aid,entries):
            async with slots:
                await self.reconcile_account(aid,entries,lobby_for)
        results=await asyncio.gather(*(one(aid,entries) for aid,entries in grouped.items()),return_exceptions=True)
        failed=[r for r in results if isinstance(r,BaseException)]
        if failed:
            raise failed[0]

    async def reconcile_account(self,aid,entries,lobby_for):
        from .mtt_sessions import waits_for_mtt
        live=[]
        for row in entries:
            table={'table_type':row.get('TableType'),'tournament_state':row.get('TournamentState'),
                   'start':aware(row.get('StartUtc'))}
            expired = aware(row.get('PlannedEndUtc'))
            waiting = waits_for_mtt(table,now()) or (table['table_type']=='MTT' and table['tournament_state']=='registering'
                                                    and expired is not None and expired<=now())
            confirmed=row.get('LastConfirmedUtc') or row.get('RegistrationConfirmed')
            if row.get('IsParked') or (confirmed and row['Status'] in ('joining','uncertain') and not row.get('PlayTableId') and waiting):
                await asyncio.to_thread(self.repo.park_registered_entry,row['EntryId'])
                self.entries[str(row['EntryId'])]={'id':str(row['EntryId']),'account':aid,'table':str(row['TableId']),
                    'status':'joining','created':aware(row['CreatedUtc']),'confirmed':aware(row.get('LastConfirmedUtc')),
                    'session':None,'parked':True,'commands':[]}
            else:live.append(row)
        if not live:return
        entries=live
        session=await self.session_for(aid)
        lobby=await lobby_for(session)
        for row in entries:
            if str(row['TableId']).startswith('T'):
                # A registered or running tournament survives a restart: subscribe again and let the
                # server's seating messages re-attach the table bot.
                eid=str(row['EntryId']);key=str(row['TableId']);status=row['Status'] if row['Status'] in ('joining','seated') else 'joining'
                await asyncio.to_thread(self.repo.adopt_reconciled_entry,row['EntryId'],status)
                self.entries[eid]={'id':eid,'account':aid,'table':key,'status':status,'buy_in':row['BuyInAmount'],
                    'created':aware(row['CreatedUtc']),'seated':aware(row['SeatedUtc']),'commands':[],
                    'session':str(row['SessionId']) if row.get('SessionId') else None}
                session.tourneys.active.setdefault(tournament_id(key),{'format':row.get('TableType') or 'SNG','entry_idx':0,'table':None,'addon_decided':False})
                try:await session.tourneys.resume()
                except Exception as exc:log.warning('Tournament resume failed for %s / %s: %s',row['Username'],key,exc)
                # A registration EvenBet no longer holds (unregistered or cancelled) and that has no
                # table is over: close it so it stops counting toward the tournament target.
                if status=='joining' and not session.tourneys.active.get(tournament_id(key),{}).get('table'):
                    try:registered=await session.tourneys.registered_tournaments()
                    except Exception as exc:
                        registered=None;log.warning('Tournament registrations unknown for %s: %s',row['Username'],exc)
                    if registered is not None and tournament_id(key) not in registered:
                        self.entries[eid].update(status='closed',closed=now(),confirmed=now())
                        session.tourneys.forget(tournament_id(key))
                        log.info('Restart closed tournament entry no longer registered: %s / %s',row['Username'],key)
                        continue
                log.info('Restart kept tournament entry: %s / %s',row['Username'],key)
                continue
            tid=table_key(row['TableId'])
            info=dict(lobby.get(tid) or session.client.tables.get(tid) or {'i':tid,'n':str(tid)})
            status=await session.adopt(tid,info)
            if status is None:
                await asyncio.to_thread(self.repo.close_reconciled_entry,row['EntryId'])
                log.info('Restart reconciliation confirmed absent: %s / %s',row['Username'],tid)
                continue
            await asyncio.to_thread(self.repo.adopt_reconciled_entry,row['EntryId'],status)
            eid=str(row['EntryId'])
            self.entries[eid]={'id':eid,'account':aid,'table':str(row['TableId']),'status':status,
                'buy_in':row['BuyInAmount'],'created':aware(row['CreatedUtc']),'seated':aware(row['SeatedUtc']),
                'confirmed':now(),'session':str(row['SessionId']) if row['SessionId'] else None,'commands':[]}
            if row['SessionId'] and not any(str(s['id'])==str(row['SessionId']) for s in self.sessions):
                self.sessions.append({'id':str(row['SessionId']),'account':aid,
                    'start':aware(row['StartedUtc']),'end':aware(row['PlannedEndUtc'])})
            log.info('Restart reconciliation adopted seat: %s / %s (%s)',row['Username'],tid,status)
        await self.close_session_if_idle(aid)

    async def run(self):
        await self.configuration()
        await self.decision.require_hand_history(self.platform)
        self.end=now()+timedelta(seconds=self.args.seconds)
        try:
            if self.args.probe:
                await self.refresh()
                result={'tables':self.tables,'existing_owned_seats':{tid:[p['name'] for p in players if p['account']] for tid,players in self.seats.items()}}
                (self.output/'probe.json').write_text(json.dumps(result,default=str,indent=2),encoding='utf-8')
                print(json.dumps(result,default=str,indent=2))
                return
            await self.repo_start()
            await self.refresh();await self.publish()
            # Persist the temporary schedule only after logins and telemetry work.
            self.saved=await asyncio.to_thread(self.repo.prepare_schedule,now(),self.end,self.args.target,self.args.high_only)
            next_refresh=next_plan=next_live_view=0
            while now()<self.end:
                tick=time.monotonic()
                if (self.output/'stop').exists(): break
                if tick>=next_refresh:
                    await self.refresh();await self.publish();next_refresh=tick+self.config['site']['LobbyRefreshSeconds']
                    next_live_view=tick+1
                elif tick>=next_live_view:
                    # Detailed telemetry is opt-in and short-lived. Keep it
                    # responsive without increasing the low-cost lobby cadence.
                    active_requests=await self.publish_live_views()
                    next_live_view=tick+(1 if active_requests else 2)
                if tick>=next_plan:
                    await self.schedule();next_plan=time.monotonic()+120
                while self.pending and self.pending[0][0]<=now().timestamp():
                    _,cid,eid,kind=heapq.heappop(self.pending);e=self.entries[eid]
                    cmd=next(x for x in e['commands'] if x['id']==cid)
                    if cmd['status']!='pending': continue
                    if kind=='cancel_join':
                        e.update(status='closed',closed=now());cmd['status']='cancelled';continue
                    self.jobs[eid]=asyncio.create_task(self.execute(e,cmd,kind))
                for e in list(self.entries.values()):
                    if e['status'] not in ('seated','sitting_out'): continue
                    s=next(s for s in self.sessions if s['id']==e['session'])
                    c=self.clients[e['account']]
                    if ring_table(e) is None: continue # Tournaments are played to the end.
                    if now()>=s['end'] or table_key(e['table']) in c.failures or -1 in c.failures:
                        self.start_leave(e)
                await asyncio.sleep(.25)
        finally:
            self.pending.clear()
            if self.started:
                for e in self.entries.values():
                    if e['status']=='reserved':
                        e.update(status='closed',closed=now())
                        for cmd in e['commands']:
                            if cmd['status']=='pending': cmd['status']='cancelled'
                # Finish joins already in flight before starting their departures.
                await asyncio.gather(*self.jobs.values(),return_exceptions=True)
                for e in self.entries.values():
                    if e['status'] in ('seated','sitting_out','joining','draining'): self.start_leave(e)
                while any(not j.done() for j in self.jobs.values()):
                    try: await self.refresh();await self.publish()
                    except Exception: log.exception('Final telemetry failed')
                    await asyncio.sleep(2)
                await asyncio.gather(*self.jobs.values(),return_exceptions=True)
                try: await self.refresh();await self.publish(ready=False)
                finally:
                    if self.saved: await asyncio.to_thread(self.repo.restore,self.saved)
            for c in list(self.clients.values()): await c.close()
            self.clients.clear()
            await self.decision.close()
            log.info('Pilot finished. Status: %s',self.output/'status.json')

    async def repo_start(self):
        await asyncio.to_thread(self.repo.start,self.args.capacity);self.started=True

    def start_leave(self,e):
        e['status']='draining'
        # Claimed from the start: a publish before the job runs must not offer it to claim_due.
        cmd={'id':guid(),'kind':'leave','status':'claimed','due':now(),'version':self.config['site']['ConfigVersion']}
        e['commands'].append(cmd)
        self.jobs[e['id']]=asyncio.create_task(self.execute_serialized(e,cmd,'leave'))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--target',type=int,default=24)
    p.add_argument('--capacity',type=int,default=30)
    p.add_argument('--seconds',type=int,default=1800)
    p.add_argument('--seed',type=int,default=17)
    p.add_argument('--high-only',action='store_true',help='Explicitly authorized temporary high-tier pilot')
    p.add_argument('--probe',action='store_true',help='Read-only login/table probe; no seats or SQL changes')
    p.add_argument('--cloud-worker',action='store_true',help='Execute Azure-planned commands; never plan locally')
    p.add_argument('--leave-on-exit',action='store_true',
                   help='Cloud worker leaves every table on shutdown instead of preserving seats for restart')
    p.add_argument('--output',default='artifacts/live-pilot')
    args=p.parse_args()
    if not 1<=args.target<=30 or not 1<=args.capacity<=30:
        p.error('Target and capacity must be between 1 and 30')
    if args.cloud_worker:
        if args.seconds<0: p.error('Cloud-worker seconds must be zero (continuous) or positive')
    elif not args.target<=args.capacity or not 60<=args.seconds<=3600:
        p.error('Pilot allows 1–30 entries, capacity up to 30, duration 60–3600 seconds')
    output=Path(args.output);output.mkdir(parents=True,exist_ok=True)
    if (output/'stop').exists(): p.error('Choose a fresh output directory; this one has a stop marker')
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s',
        handlers=[logging.FileHandler(output/'pilot.log',encoding='utf-8'),logging.StreamHandler()])
    for name in ('httpx','httpcore','websockets'): logging.getLogger(name).setLevel(logging.WARNING)
    pilot=Pilot(args)
    asyncio.run(pilot.run_cloud_worker() if args.cloud_worker else pilot.run())

if __name__=='__main__': main()
