"""After an uncontested win, a qualifying hand is shown with the account's show-cards chance."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
import random

import pytest

from schedule_controller.pilot import Session


class Control:
    def __init__(self,manual):self.manual=manual
    async def manual_enabled(self):return self.manual


def offered(hole='40;41',board='42;20;0'): # Queens full of... qd qh with qs on board: a set.
    return {'gi':9,'gt':72,'ts':2,'aa':[10,16],'m':{'ai':0,'r':5},'d':{'c':board},'s':[{'i':1,'c':900,'d':hole}]}


def session_with(chance,manual=False,state=None):
    session=Session.__new__(Session);session.repo=None
    session.policy={'Username':'kate','ShowCardsChance':chance}
    session.rng=random.Random(2)
    session.client=SimpleNamespace(states={7:state or offered()},tables={7:{'g':72}},play=AsyncMock())
    session.bots={7:SimpleNamespace(control=Control(manual))}
    return session


@pytest.fixture(autouse=True)
def no_delay(monkeypatch):
    monkeypatch.setattr(asyncio,'sleep',AsyncMock())


def test_qualifying_hand_is_shown():
    session=session_with(1)
    asyncio.run(session.maybe_show(7,offered(),0))
    session.client.play.assert_awaited_once_with(7,10,0)


@pytest.mark.parametrize('chance,manual,hole', [
    (0,False,'40;41'),   # chance says no
    (1,True,'40;41'),    # a human is in control
    (1,False,'40;5'),    # Qc 3d on Qh 7c 2c: an honest one pair is neither strong nor a bluff
])
def test_not_shown(chance,manual,hole):
    state=offered(hole=hole)
    session=session_with(chance,manual,state)
    asyncio.run(session.maybe_show(7,state,0))
    session.client.play.assert_not_awaited()


def test_expired_offer_is_not_answered():
    session=session_with(1,state=offered()|{'aa':[]})
    asyncio.run(session.maybe_show(7,offered(),0))
    session.client.play.assert_not_awaited()


def test_show_rate_follows_the_chance():
    shown=0
    for seed in range(400):
        session=session_with(.25);session.rng=random.Random(seed)
        asyncio.run(session.maybe_show(7,offered(),0))
        shown+=session.client.play.await_count
    assert 70<shown<130
