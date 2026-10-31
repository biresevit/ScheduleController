"""Breaks: one simulated person sits out on all tables at once, keeps the seats, and comes back."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
import random

import pytest

from schedule_controller.pilot import Pilot,Session


class Control:
    def __init__(self,manual):self.manual=manual
    async def manual_enabled(self):return self.manual


def session_with(tables,manual=()):
    session=Session.__new__(Session);session.repo=None
    session.policy={'Username':'kate','BreakAfterMinSeconds':1200,'BreakAfterMaxSeconds':3600,'BreakMinSeconds':10,'BreakMaxSeconds':300}
    session.rng=random.Random(5);session.failures={};session.break_until=None
    session.ready=asyncio.Event();session.ready.set()
    states={tid:{'s':[{'i':1,'c':5000,'f':1}]} for tid in tables} # The server has applied the sit-out.
    session.client=SimpleNamespace(states=states,hero_seat=lambda s:0,wait_state=AsyncMock(),
                                   sit_out_next_hand=AsyncMock(),sit_back=AsyncMock())
    session.bots={tid:SimpleNamespace(leaving=False,busted=False,on_break=False,control=Control(tid in manual)) for tid in tables}
    session.queues={tid:object() for tid in tables}
    session.changes=[];session.on_break_change=lambda t,away:session.changes.append((sorted(t),away))
    return session


def test_break_sits_out_every_table_together_then_returns(monkeypatch):
    session=session_with([1,2,3],manual=[3])
    slept=[]
    async def sleep(seconds):
        slept.append(seconds)
        assert all(session.bots[t].on_break for t in (1,2)) and not session.bots[3].on_break
    monkeypatch.setattr(asyncio,'sleep',sleep)
    asyncio.run(session.take_break())
    assert sorted(c.args[0] for c in session.client.sit_out_next_hand.await_args_list)==[1,2]
    assert sorted(c.args[0] for c in session.client.sit_back.await_args_list)==[1,2]
    assert 10<=slept[0]<=300
    assert session.changes==[([1,2],True),([1,2],False)]
    assert not any(b.on_break for b in session.bots.values()) and session.break_until is None


def test_a_leave_during_the_break_wins_over_returning(monkeypatch):
    session=session_with([1,2])
    async def sleep(seconds):session.bots[2].leaving=True
    monkeypatch.setattr(asyncio,'sleep',sleep)
    asyncio.run(session.take_break())
    assert [c.args[0] for c in session.client.sit_back.await_args_list]==[1]
    assert session.changes[-1]==([1],False)


def test_logging_out_during_a_break_never_presses_im_back(monkeypatch):
    session=session_with([1])
    async def sleep(seconds):raise asyncio.CancelledError()
    monkeypatch.setattr(asyncio,'sleep',sleep)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(session.take_break())
    session.client.sit_back.assert_not_awaited()
    assert not session.bots[1].on_break


def test_break_clock_runs_only_while_playing(monkeypatch):
    session=session_with([])
    session.next_break_after=lambda:12
    session.take_break=AsyncMock()
    ticks=[0]
    async def sleep(seconds):
        ticks[0]+=1
        if ticks[0]==4: # Joined a table after three idle ticks.
            session.bots[1]=SimpleNamespace(leaving=False,busted=False,on_break=False,control=None);session.queues[1]=object()
        if ticks[0]>8:raise asyncio.CancelledError()
    monkeypatch.setattr(asyncio,'sleep',sleep)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(session.run_breaks(tick=5))
    assert session.take_break.await_count==1 # Due after three playing ticks (15 s >= 12 s), not after idle time.


def test_breaks_are_mostly_short_and_within_the_accounts_range():
    session=session_with([])
    lengths=[session.break_length() for _ in range(2000)]
    assert min(lengths)>=10 and max(lengths)<=300
    assert sum(l<100 for l in lengths)>sum(l>200 for l in lengths)*3
    after=[session.next_break_after() for _ in range(200)]
    assert min(after)>=1200 and max(after)<=3600


def test_break_shows_seats_as_sitting_out_and_back():
    p=Pilot.__new__(Pilot)
    p.entries={'a':{'account':1,'table':'7','status':'seated'},'b':{'account':1,'table':'8','status':'draining'},
               'c':{'account':2,'table':'7','status':'seated'}}
    p.break_change(1,[7,8],True)
    assert [e['status'] for e in p.entries.values()]==['sitting_out','draining','seated']
    p.break_change(1,[7],False)
    assert p.entries['a']['status']=='seated'
