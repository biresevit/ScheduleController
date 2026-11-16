"""MTT pacing is independent of ring planning and manual/wake-up commands."""
from datetime import timedelta
import random

from schedule_controller.cloud_scheduler import CloudScheduler
from test_mtt_plans import START


class Database:
    def __init__(self, last=None, rate_limited=0):
        self.last, self.rate_limited = last, rate_limited
        self.queries = []

    def rows(self, sql, *params):
        self.queries.append(sql)
        return []

    def one(self, sql, *params):
        self.queries.append(sql)
        if 'LastPlannedUtc' in sql:
            return {'LastPlannedUtc': self.last}
        if 'RATE[_]LIMIT' in sql:
            return {'n': self.rate_limited}
        raise AssertionError(sql)


def run(db, monkeypatch, next_time=None):
    import schedule_controller.cloud_scheduler as module
    calls = []
    original = module.plan_mtt_registrations

    def record(*args, **kwargs):
        calls.append(kwargs['max_actions'])
        return original(*args, **kwargs)

    monkeypatch.setattr(module, 'plan_mtt_registrations', record)
    site = dict(SiteId=1, TableFreshnessSeconds=45, MinJoinGapSeconds=20, MaxJoinGapSeconds=60,
                HeartbeatTimeoutSeconds=60, NextMttUtc=next_time)
    result = CloudScheduler()._plan_tournaments(db, site, START, {}, [], [], [], {}, {},
                                               random.Random(1), formats=('MTT',))
    assert 'SNG' not in result
    return result['MTT'], calls


def test_repeated_triggers_do_not_create_another_batch(monkeypatch):
    result, calls = run(Database(last=START - timedelta(seconds=10)), monkeypatch)
    assert result['reasons'] == ['batch_cooldown'] and not calls


def test_normal_timer_jitter_does_not_lose_a_registration_pass(monkeypatch):
    result, calls = run(Database(last=START - timedelta(seconds=29)), monkeypatch)
    assert calls == [5] and result['reasons'] == ['on_plan']


def test_recent_rate_limit_keeps_existing_backoff(monkeypatch):
    result, calls = run(Database(rate_limited=1), monkeypatch, START + timedelta(seconds=20))
    assert result['reasons'] == ['rate_limit_backoff'] and not calls


def test_rate_limit_limits_recovery_to_one_registration(monkeypatch):
    _, calls = run(Database(rate_limited=1), monkeypatch)
    assert calls == [1]
