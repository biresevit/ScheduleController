import pytest

from schedule_controller.human_seat_history import sample


@pytest.mark.parametrize('fail', [False, True])
def test_sampler_commits_or_rolls_back_and_always_closes(fail):
    calls = []
    class Connection:
        def cursor(self): return self
        def execute(self, sql):
            calls.append(sql)
            if fail: raise RuntimeError('SQL unavailable')
        def commit(self): calls.append('commit')
        def rollback(self): calls.append('rollback')
        def close(self): calls.append('close')
    if fail:
        with pytest.raises(RuntimeError): sample(Connection)
    else:
        sample(Connection)
    assert calls == ['EXEC backoffice.SampleHumanSeats', 'rollback' if fail else 'commit', 'close']
