"""Collect anonymous human-seat counts from already-published lobby observations."""
from .database import connection_string


def sample(connect=None):
    if connect is None:
        import pyodbc
        connect = lambda: pyodbc.connect(connection_string(), autocommit=False, timeout=15)
    conn = connect()
    try:
        conn.timeout = 15
        conn.cursor().execute('EXEC backoffice.SampleHumanSeats')
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
