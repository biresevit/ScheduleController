import asyncio
import logging
import time
from types import SimpleNamespace

import pytest

from schedule_controller.worker_diagnostics import phase,heartbeat_stall


def test_stall_identifies_awaited_phase_without_arguments_or_stack_locals(caplog):
    async def scenario():
        worker=SimpleNamespace()
        entered=asyncio.Event()
        async def publish():
            secret_password='never-log-this-secret'
            with phase(worker,'publish'):
                entered.set()
                await asyncio.sleep(3600)
            return secret_password
        main=asyncio.create_task(publish())
        await entered.wait()
        heartbeat_stall(worker,main,124,.02)
        main.cancel()
        with pytest.raises(asyncio.CancelledError):await main
        assert worker.diagnostic_phase is None
    with caplog.at_level(logging.INFO):asyncio.run(scenario())
    assert 'event=heartbeat_stall phase=publish' in caplog.text
    assert 'heartbeat_age_seconds=124.000' in caplog.text
    assert 'event_loop_lag_seconds=0.020' in caplog.text
    assert 'awaiting=publish,sleep' in caplog.text
    assert 'never-log-this-secret' not in caplog.text


def test_cleanup_progress_marks_both_start_and_finish_on_error(caplog):
    worker=SimpleNamespace(diagnostic_phase=('publish',time.monotonic()))
    original=worker.diagnostic_phase
    with caplog.at_level(logging.INFO):
        with pytest.raises(RuntimeError):
            with phase(worker,'cleanup_live_views'):
                raise RuntimeError('private exception message')
    assert worker.diagnostic_phase==original
    assert 'event=phase_started phase=cleanup_live_views' in caplog.text
    assert 'event=phase_finished phase=cleanup_live_views' in caplog.text
    assert 'outcome=interrupted' in caplog.text
    assert 'private exception message' not in caplog.text


def test_phase_names_are_fixed():
    with pytest.raises(ValueError):
        with phase(SimpleNamespace(),'private-account-name'):pass
