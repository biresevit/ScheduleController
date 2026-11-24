"""Local worker progress evidence; no account data, arguments, paths or stack locals."""
import logging
import time
from contextlib import contextmanager

log=logging.getLogger(__name__)
PHASES=frozenset(('startup_configuration','startup_decision','startup_reconciliation','refresh','publish',
    'claim','resource_telemetry','decision_metrics','cleanup_watchdog','cleanup_live_views','cleanup_manual',
    'cleanup_chat','cleanup_fenced','cleanup_clients','cleanup_decision','idle'))
AWAIT_NAMES=frozenset(('run_cloud_worker','configuration','require_hand_history','reconcile_cloud_entries',
    'refresh','publish','to_thread','wait_for','wait','gather','acquire','sleep','close','stop_fenced',
    'leave_unreported','run_live_views','run_manual','run_chat','request','send','recv','receive',
    'ensure_login','login','connect','open','table','lobby','snapshot','_wait','_wait_for_data'))


@contextmanager
def phase(worker,name):
    if name not in PHASES:raise ValueError('Unknown worker diagnostic phase')
    previous=getattr(worker,'diagnostic_phase',None)
    started=time.monotonic()
    worker.diagnostic_phase=(name,started)
    if name.startswith(('cleanup_','startup_')):
        log.info('Worker diagnostic event=phase_started phase=%s',name)
    outcome='ok'
    try:
        yield
    except BaseException:
        outcome='interrupted'
        raise
    finally:
        elapsed=max(0,time.monotonic()-started)
        if elapsed>=10 or outcome!='ok' or name.startswith(('cleanup_','startup_')):
            log.info('Worker diagnostic event=phase_finished phase=%s elapsed_seconds=%.3f outcome=%s',
                     name,elapsed,outcome)
        worker.diagnostic_phase=previous


def heartbeat_stall(worker,main,silent,event_loop_lag):
    name,started=getattr(worker,'diagnostic_phase',None) or ('idle',time.monotonic())
    chain=[]
    current=main.get_coro() if main is not None and hasattr(main,'get_coro') else None
    for _ in range(32):
        if current is None:break
        code=getattr(current,'cr_code',None) or getattr(current,'gi_code',None)
        function=getattr(code,'co_name','')
        if function in AWAIT_NAMES:chain.append(function)
        current=getattr(current,'cr_await',None) or getattr(current,'gi_yieldfrom',None)
    log.error('Worker diagnostic event=heartbeat_stall phase=%s phase_age_seconds=%.3f '
              'heartbeat_age_seconds=%.3f event_loop_lag_seconds=%.3f awaiting=%s',
              name if name in PHASES else 'idle',max(0,time.monotonic()-started),
              max(0,silent),max(0,event_loop_lag),','.join(chain[:8]) or 'unknown')
