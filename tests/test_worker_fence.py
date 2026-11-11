"""A worker whose heartbeat no longer reaches the database stops instead of playing seats nobody sees."""
import asyncio
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from schedule_controller import pilot as pilot_module
from schedule_controller import pilot_repository
from schedule_controller.pilot import Pilot, WorkerFenced
from schedule_controller.pilot_repository import Repository


def client(name, ring=(), tournament=()):
    leave = AsyncMock()
    return SimpleNamespace(policy={'Username': name}, workers={t: None for t in (*ring, *tournament)},
                           tourneys=SimpleNamespace(by_table={t: None for t in tournament}),
                           remove=AsyncMock(), client=SimpleNamespace(leave=leave), close=AsyncMock())


def bare_pilot():
    p = Pilot.__new__(Pilot)
    p.fenced = None; p.jobs = {}; p.clients = {}; p.last_heartbeat = time.monotonic()
    p.repo = SimpleNamespace(owns_machine=lambda: True)
    return p


def test_watchdog_stops_the_worker_once_the_heartbeat_is_older_than_the_fence():
    async def scenario():
        p = bare_pilot(); p.last_heartbeat = time.monotonic() - 1
        main = asyncio.create_task(asyncio.sleep(60))
        await p.watch_heartbeat(main, fence=0.5, every=0.01)
        await asyncio.gather(main, return_exceptions=True)
        assert main.cancelled() and p.fenced >= 0.5
    asyncio.run(scenario())


def test_watchdog_leaves_a_reporting_worker_alone():
    async def scenario():
        p = bare_pilot()
        main = asyncio.create_task(asyncio.sleep(60))
        async def heartbeat():
            while True:
                p.last_heartbeat = time.monotonic(); await asyncio.sleep(0.01)
        beat = asyncio.create_task(heartbeat())
        watch = asyncio.create_task(p.watch_heartbeat(main, fence=0.1, every=0.01))
        await asyncio.sleep(0.3)
        assert not main.done() and p.fenced is None
        for t in (watch, beat, main): t.cancel()
        await asyncio.gather(watch, beat, main, return_exceptions=True)
    asyncio.run(scenario())


def test_ring_tables_are_left_after_the_hand_and_tournament_seats_are_kept():
    async def scenario():
        p = bare_pilot()
        a = client('andrew', ring=('r1', 'r2'), tournament=('t1',)); p.clients = {1: a}
        await p.leave_unreported()
        assert sorted(c.args for c in a.remove.await_args_list) == [('r1',), ('r2',)]
        assert all(c.kwargs == {'finish': True} for c in a.remove.await_args_list)
        a.client.leave.assert_not_awaited()
    asyncio.run(scenario())


def test_a_hand_that_does_not_finish_in_time_leaves_at_once(monkeypatch):
    monkeypatch.setattr(pilot_module, 'FENCE_FINISH_SECONDS', 0.05)
    async def scenario():
        p = bare_pilot()
        a = client('andrew', ring=('r1',))
        async def endless_hand(*args, **kwargs): await asyncio.sleep(10)
        a.remove = AsyncMock(side_effect=endless_hand)
        b = client('bob', ring=('r2',)); b.client.leave.side_effect = ConnectionError('gone')
        b.remove.side_effect = RuntimeError('socket closed')
        p.clients = {1: a, 2: b}
        await p.leave_unreported()  # one account's failure never stops the others
        a.client.leave.assert_awaited_once_with('r1')
        b.client.leave.assert_awaited_once_with('r2')
    asyncio.run(scenario())


@pytest.mark.parametrize('owns,left', [(lambda: True, False), (lambda: False, True), (None, True)])
def test_seats_are_kept_only_while_this_worker_still_owns_the_machine(owns, left, monkeypatch):
    monkeypatch.setattr(pilot_module, 'FENCE_JOBS_SECONDS', 0.05)
    async def scenario():
        p = bare_pilot(); p.fenced = 130
        def unreachable(): raise ConnectionError('login timeout')
        p.repo = SimpleNamespace(owns_machine=owns or unreachable)
        stuck = asyncio.Event()
        async def job():
            try: await stuck.wait()
            finally: await stuck.wait()  # e.g. a job's final publish waits behind the stuck one
        p.jobs = {'e1': asyncio.create_task(job())}
        p.leave_unreported = AsyncMock()
        await asyncio.wait_for(p.stop_fenced(), 15)
        assert p.leave_unreported.await_count == (1 if left else 0)
        p.jobs['e1'].cancel()
    asyncio.run(scenario())


def test_a_fenced_worker_publishes_nothing():
    async def scenario():
        p = bare_pilot(); p.fenced = 125
        p.publish_lock = None  # never reached
        await p.publish()
    asyncio.run(scenario())


def test_a_worker_stuck_on_the_database_stops_leaves_and_raises_for_a_restart(monkeypatch, tmp_path):
    monkeypatch.setattr(pilot_module, 'FENCE_SECONDS', 0.4)
    monkeypatch.setattr(pilot_module, 'WATCH_SECONDS', 0.05)
    monkeypatch.setattr(pilot_module, 'HEARTBEAT_SECONDS', 0.1)
    class Lock:
        def __init__(self, *a): pass
        def acquire(self): return False
        def release(self): self.released = True
    monkeypatch.setattr(pilot_module, 'WorkerLock', Lock)
    import schedule_controller.chat_worker as chat_worker
    import schedule_controller.manual_worker as manual_worker
    idle = lambda pilot: asyncio.sleep(3600)
    monkeypatch.setattr(chat_worker, 'run_chat', idle)
    monkeypatch.setattr(manual_worker, 'run_manual', idle)

    async def scenario():
        p = Pilot.__new__(Pilot)
        p.args = SimpleNamespace(seconds=0, output=str(tmp_path), stop_file=str(tmp_path / 'stop'), leave_on_exit=False)
        p.output = Path(tmp_path); p.fenced = None; p.jobs = {}; p.started = False
        p.last_heartbeat = time.monotonic()
        p.config = {'site': {'LobbyRefreshSeconds': 3600}}
        p.configuration = AsyncMock(); p.decision = SimpleNamespace(require_hand_history=AsyncMock(), close=AsyncMock())
        p.reconcile_cloud_entries = AsyncMock(); p.refresh = AsyncMock(); p.run_live_views = lambda: asyncio.sleep(3600)
        p.platform = None
        released = threading.Event()
        p.repo = SimpleNamespace(start_cloud_worker=lambda *a: [], finish_cloud_worker_reconciliation=lambda: None,
                                 claim_due=lambda n: [], owns_machine=lambda: False, stop_cloud_worker=lambda: None)
        a = client('andrew', ring=('r1',)); p.clients = {1: a}
        publishes = []
        async def publish(ready=True):
            publishes.append(ready)
            if len(publishes) == 1:
                p.last_heartbeat = time.monotonic(); return
            await asyncio.Event().wait()  # the database call that never returns
        p.publish = publish
        with pytest.raises(WorkerFenced, match='No heartbeat reached the database'):
            await asyncio.wait_for(p.run_cloud_worker(), 10)
        assert len(publishes) >= 2  # the loop's heartbeat publish is the one that hung
        a.remove.assert_awaited_once_with('r1', finish=True)
        a.close.assert_awaited_once()
        assert not p.clients
    monkeypatch.setattr(Pilot, 'platform', None, raising=False)
    asyncio.run(scenario())


def test_worker_connections_have_a_query_timeout(monkeypatch):
    conns = []
    class Conn:
        def cursor(self): return SimpleNamespace(execute=lambda *a: None, close=lambda: None)
        def commit(self): pass
        def close(self): pass
    monkeypatch.setitem(sys.modules, 'pyodbc', SimpleNamespace(connect=lambda *a, **k: conns.append(Conn()) or conns[-1]))
    monkeypatch.setattr(pilot_repository, 'connection_string', lambda: 'DRIVER=x')
    pilot_repository._open_connection()
    assert conns[0].timeout == pilot_repository.QUERY_TIMEOUT_SECONDS < pilot_module.FENCE_SECONDS


@pytest.mark.parametrize('stored,owned', [('ABC', True), ('other', False), (None, False)])
def test_owns_machine_compares_the_epoch_on_a_new_connection(monkeypatch, stored, owned):
    seen = []
    class Conn:
        def cursor(self):
            return SimpleNamespace(execute=lambda sql, *a: seen.append(a) or SimpleNamespace(
                fetchone=lambda: None if stored is None else (stored,)))
        def close(self): self.closed = True
    monkeypatch.setitem(sys.modules, 'pyodbc', SimpleNamespace(connect=lambda *a, **k: Conn()))
    monkeypatch.setattr(pilot_repository, 'connection_string', lambda: 'DRIVER=x')
    assert Repository('abc', 1006, 1023).owns_machine() is owned
    assert seen == [(1006, 1023)]


def seated_session(monkeypatch):
    from schedule_controller.pilot import Session
    import schedule_controller.pilot as module
    class FakeBot:
        async def process(self, state): pass
    monkeypatch.setattr(module, 'Bot', lambda *args, **kwargs: FakeBot())
    client = SimpleNamespace(tables={}, states={42: {'s': [{'f': 0}]}}, join=AsyncMock(), hero_seat=lambda value: 0,
                             return_from_away=AsyncMock(), player_command=AsyncMock(), sit=AsyncMock())
    session = Session.__new__(Session); session.repo = None; session.failures = {}
    session.ready = asyncio.Event(); session.ready.set(); session.policy = {'Username': 'Fede'}
    session.client = client; session.decision = object(); session.bots = {}; session.queues = {}; session.workers = {}
    return session, client


def test_a_takeover_join_resumes_the_seat_the_account_already_holds(monkeypatch):
    async def scenario():
        session, client = seated_session(monkeypatch)
        assert await session.add(42, {'i': 42, 'n': 'Prague'}, random.Random(1), takeover=True) is None
        client.sit.assert_not_awaited()
        assert 42 in session.workers
        session.workers[42].cancel(); await asyncio.gather(session.workers[42], return_exceptions=True)
    import random
    asyncio.run(scenario())


def test_an_ordinary_join_still_refuses_an_unexpected_existing_seat(monkeypatch):
    async def scenario():
        session, client = seated_session(monkeypatch)
        with pytest.raises(RuntimeError, match='Unexpected existing seat'):
            await session.add(42, {'i': 42, 'n': 'Prague'}, random.Random(1))
        assert not session.workers
    import random
    asyncio.run(scenario())


def test_release_labels_and_takeover_platforms():
    from schedule_controller.cloud_scheduler import release_at_least, takes_over_seats, TAKEOVER_RELEASE
    assert release_at_least('2026.11.12.1', TAKEOVER_RELEASE) and release_at_least('2026.11.13.0', TAKEOVER_RELEASE)
    assert not release_at_least('2026.11.11.13', TAKEOVER_RELEASE) and not release_at_least(None, TAKEOVER_RELEASE)
    assert takes_over_seats({'Platform': 'BluffPokerPlatform'}) and not takes_over_seats({'Platform': 'EvenBet'})
