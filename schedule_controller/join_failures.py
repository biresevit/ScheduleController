"""Session-wide circuit breaker for ring joins and tournament registrations."""

MAX_JOIN_FAILURES = 2
LIMIT_REASON = 'join_failure_limit'
LIMIT_DESCRIPTION = 'Session ended after two failed table joins or tournament registrations; log in again after the session cooldown.'


def limit_failed_sessions(db, site_id):
    """Use persisted outcomes, so restarts and different table IDs cannot reset the limit.

    Existing seats drain through the normal session-end planner. Tournament seats
    finish normally. Never release an uncertain seat or an account with open entries.
    """
    db.execute("""UPDATE s SET EndReason='join_failure_limit',
            PlannedEndUtc=CASE WHEN s.StartedUtc>SYSUTCDATETIME() THEN s.StartedUtc ELSE SYSUTCDATETIME() END
        FROM scheduler.AccountSession s
        WHERE s.SiteId=? AND s.IsOpen=1 AND COALESCE(s.EndReason,'')<>'join_failure_limit'
          AND (SELECT COUNT(*) FROM scheduler.Command c JOIN scheduler.Entry e ON e.EntryId=c.EntryId
               WHERE e.SessionId=s.SessionId AND c.Kind='join' AND c.Status='failed')>=2""", site_id)
    db.execute("""UPDATE c SET Status='cancelled',IsOpen=0,ResultCode='join_failure_limit',
            ResultText=?,UpdatedUtc=SYSUTCDATETIME()
        FROM scheduler.Command c JOIN scheduler.Entry e ON e.EntryId=c.EntryId
        JOIN scheduler.AccountSession s ON s.SessionId=e.SessionId
        WHERE s.SiteId=? AND s.EndReason='join_failure_limit' AND c.Kind='join'
          AND c.Status='pending' AND c.IsOpen=1 AND e.Status='reserved'""", LIMIT_DESCRIPTION, site_id)
    db.execute("""UPDATE e SET Status='closed',IsOpen=0,ClosedUtc=SYSUTCDATETIME()
        FROM scheduler.Entry e JOIN scheduler.AccountSession s ON s.SessionId=e.SessionId
        WHERE s.SiteId=? AND s.EndReason='join_failure_limit' AND e.Status='reserved' AND e.IsOpen=1
          AND NOT EXISTS(SELECT 1 FROM scheduler.Command c WHERE c.EntryId=e.EntryId AND c.IsOpen=1)""", site_id)
    db.execute("""UPDATE s SET IsOpen=0,
            ActualEndUtc=CASE WHEN s.StartedUtc>SYSUTCDATETIME() THEN s.StartedUtc ELSE SYSUTCDATETIME() END
        FROM scheduler.AccountSession s WHERE s.SiteId=? AND s.IsOpen=1 AND s.EndReason='join_failure_limit'
          AND NOT EXISTS(SELECT 1 FROM scheduler.Entry e WHERE e.SessionId=s.SessionId AND e.IsOpen=1)""", site_id)
    db.execute("""DELETE l FROM scheduler.AccountLease l
        WHERE l.SiteId=? AND l.RequiresReconciliation=0
          AND EXISTS(SELECT 1 FROM scheduler.AccountSession s WHERE s.BotAccountId=l.BotAccountId
                     AND s.EndReason='join_failure_limit' AND s.IsOpen=0)
          AND NOT EXISTS(SELECT 1 FROM scheduler.AccountSession s WHERE s.BotAccountId=l.BotAccountId AND s.IsOpen=1)
          AND NOT EXISTS(SELECT 1 FROM scheduler.Entry e WHERE e.BotAccountId=l.BotAccountId AND e.IsOpen=1)""", site_id)


def failure_description(exc, action, username, table, phase):
    # Protocol exceptions can contain request data; never include login credentials.
    detail = str(exc)
    if isinstance(exc, KeyError):
        detail = f'Missing table or response field {detail}; the expected lobby/table data was unavailable.'
    elif not detail:
        detail = 'No response detail was supplied.'
    return f'{action} failed for {username} at table {table} during {phase}: {type(exc).__name__}: {detail}'[:1000]
