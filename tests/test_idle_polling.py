import asyncio
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from schedule_controller import manual_worker
from schedule_controller.chat_repository import ChatRepository
from schedule_controller.pilot_repository import Repository


def test_manual_polling_stays_fast_while_a_view_is_open_and_returns_to_idle(monkeypatch):
    async def go():
        delays=[]
        rows=iter([[],[dict(BotAccountId=7,TableId='42',Enabled=True,Pending=False)],[],[]])
        async def sleep(delay):
            delays.append(delay)
            if len(delays)==4:raise asyncio.CancelledError()
        monkeypatch.setattr(manual_worker.asyncio,'sleep',sleep)
        pilot=SimpleNamespace(clients={},live_requests_seen=1.0,repo=SimpleNamespace(manual_watches=lambda:next(rows)))
        with pytest.raises(asyncio.CancelledError):await manual_worker.run_manual(pilot)
        assert delays==[3,.25,.25,3]
    asyncio.run(go())


def test_manual_polling_skips_the_database_while_no_live_view_is_open(monkeypatch):
    async def go():
        delays=[];calls=[]
        async def sleep(delay):
            delays.append(delay)
            if len(delays)==3:raise asyncio.CancelledError()
        monkeypatch.setattr(manual_worker.asyncio,'sleep',sleep)
        pilot=SimpleNamespace(clients={},live_requests_seen=0.0,repo=SimpleNamespace(manual_watches=lambda:calls.append(1) or []))
        with pytest.raises(asyncio.CancelledError):await manual_worker.run_manual(pilot)
        assert calls==[] and delays==[3,3,3]
    asyncio.run(go())


def test_manual_polling_is_slow_while_a_view_is_open_with_control_off(monkeypatch):
    async def go():
        delays=[]
        off=[dict(BotAccountId=7,TableId='42',Enabled=False,Pending=False)]
        async def sleep(delay):
            delays.append(delay)
            if len(delays)==2:raise asyncio.CancelledError()
        monkeypatch.setattr(manual_worker.asyncio,'sleep',sleep)
        pilot=SimpleNamespace(clients={},live_requests_seen=1.0,repo=SimpleNamespace(manual_watches=lambda:off))
        with pytest.raises(asyncio.CancelledError):await manual_worker.run_manual(pilot)
        assert delays==[1.0,1.0]
    asyncio.run(go())


class DB:
    def __init__(self):self.writes=[];self.owner_checks=0;self.claims=0
    def execute(self,sql,*args):self.writes.append(sql)
    def rows(self,sql,*args):return []
    def one(self,sql,*args):
        if 'SELECT WorkerEpoch,IsReady' in sql:
            self.owner_checks+=1;return dict(WorkerEpoch='epoch',IsReady=True)
        if 'SELECT TOP(1) c.RequestId' in sql:
            self.claims+=1;return None
        raise AssertionError(sql)


class Repo(ChatRepository):
    epoch='epoch'
    def __init__(self):self.db=DB();self.transactions=0;self.fail_commit=False
    @contextmanager
    def transaction(self):
        self.transactions+=1
        yield self.db
        if self.fail_commit:raise RuntimeError('Commit failed')


def test_chat_poll_claims_and_ingests_in_one_fenced_transaction_but_throttles_idle_writes():
    repo=Repo()
    for _ in range(3):repo.chat_exchange([],claim=True)
    assert repo.transactions==repo.db.owner_checks==repo.db.claims==3
    assert sum('UPDATE scheduler.ChatAgent' in q for q in repo.db.writes)==1
    assert sum("Status='expired'" in q for q in repo.db.writes)==1
    repo.epoch='obsolete'
    with pytest.raises(RuntimeError,match='epoch lost'):repo.chat_exchange([],claim=True)


def test_failed_chat_commit_does_not_suppress_the_next_heartbeat():
    repo=Repo();repo.fail_commit=True
    with pytest.raises(RuntimeError,match='Commit failed'):repo.chat_exchange([],claim=True)
    assert not hasattr(repo,'_chat_heartbeat_at') and not hasattr(repo,'_chat_cleanup_at')
    repo.fail_commit=False;repo.chat_exchange([],claim=True)
    assert sum('UPDATE scheduler.ChatAgent' in q for q in repo.db.writes)==2


def test_live_views_filter_expiry_on_every_read_but_cleanup_only_periodically():
    repo=Repo()
    for _ in range(3):assert Repository.live_view_requests(repo)==[]
    assert sum("Status='expired'" in q for q in repo.db.writes)==1
    repo._live_cleanup_at=0
    Repository.live_view_requests(repo)
    assert sum("Status='expired'" in q for q in repo.db.writes)==2
