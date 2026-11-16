"""Retry complete SQL-only repository transactions after a confirmed rollback.

Never wrap network poker sends with this decorator: ambiguous sends cannot be
replayed. Repository context managers roll back before this wrapper retries.
"""
from functools import wraps
import logging
import random
import time

log=logging.getLogger(__name__)


def is_transient_database_error(error):
    """Recognize SQL transport/timeouts without treating poker/network errors as SQL failures.

    This classifies errors for the worker loop, not for replaying transactions or
    poker actions. A failed repository transaction discards its SQL connection.
    """
    import pyodbc
    if not isinstance(error,pyodbc.Error) or not error.args:
        return False
    state=str(error.args[0])
    if state in {'HYT00','HYT01','40001'} or state.startswith('08'):
        return True
    # Azure SQL throttling/failover and SQL Server lock timeouts.
    text=' '.join(str(arg) for arg in error.args)
    return state in {'42000','HY000'} and any(
        f'({code})' in text for code in (1222,40197,40501,40613,10928,10929,49918,49919,49920))


def retry_deadlocks(function):
    @wraps(function)
    def run(*args,**kwargs):
        for attempt in range(6):
            try:return function(*args,**kwargs)
            except Exception as error:
                text=' '.join(str(a) for a in error.args).lower()
                deadlock=bool(error.args and str(error.args[0])=='40001') or '1205' in text and 'deadlock' in text
                if not deadlock or attempt==5:raise
                log.warning('SQL deadlock in %s; retrying rolled-back transaction (%s/6)',function.__qualname__,attempt+1)
                time.sleep(random.uniform(.05,min(1.5,.1*2**attempt)))
    return run
