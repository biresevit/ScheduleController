from decimal import Decimal

import pytest

from schedule_controller.rules import Band
from schedule_controller.timeline import simulate

LOW_ONLY = [Band("low", Decimal(".02"), Decimal(".5"), 10000),
            Band("medium", Decimal(".5"), Decimal(1), 0), Band("high", Decimal(1), Decimal(4), 0, True)]


def test_timeline_reaches_56_from_20_and_is_reproducible():
    config = {"target": 56, "current": 20, "horizon_minutes": 60, "seed": 7,
              "initial_remaining_min_minutes": 240, "initial_remaining_max_minutes": 240,
              "session_min_minutes": 240, "session_max_minutes": 240}
    result = simulate(config)
    assert result == simulate(config)
    assert result["joins"] == 36 and result["departures"] == 0
    assert result["final_entries"] == 56
    assert 0 < result["first_reached_seconds"] < 3600
    assert max(p["entries"] for p in result["points"]) <= 56
    assert all(b["seconds"] >= a["seconds"] for a, b in zip(result["events"], result["events"][1:]))


def test_one_session_ends_all_its_entries_then_scheduler_replaces_them():
    config = {"target": 2, "current": 2, "horizon_minutes": 10, "account_count": 6,
              "existing_sessions": [{"stake": "low", "entries": 2, "started_minutes_ago": 30, "ends_after_minutes": 1}],
              "min_gap_seconds": 20, "max_gap_seconds": 20, "finish_hand_seconds": 10}
    result = simulate(config, LOW_ONLY)
    left = [e for e in result["events"] if e["kind"] == "left"]
    assert len(left) == 2
    assert left[0]["account"] == left[1]["account"]
    assert {e["seconds"] for e in left} == {70}
    assert result["joins"] == 2 and result["final_entries"] == 2
    assert result["sessions"][0]["actual_end_at"] is not None
    assert result["sessions"][0]["initial_entries"] == 2


def test_unreachable_target_reports_shortfall_without_exceeding_capacity():
    result = simulate({"target": 56, "current": 0, "machine_count": 1, "machine_capacity": 10,
                       "horizon_minutes": 20})
    assert result["first_reached_seconds"] is None
    assert max(p["entries"] for p in result["points"]) <= 10
    assert any(e["kind"] == "waiting" for e in result["events"])


def test_first_check_delay_is_applied_before_join_confirmation():
    result = simulate({"target": 1, "current": 0, "first_tick_seconds": 120,
                       "min_gap_seconds": 20, "max_gap_seconds": 20, "join_latency_seconds": 5,
                       "horizon_minutes": 10}, LOW_ONLY)
    assert result["first_reached_seconds"] == 145


def test_current_above_target_drains_in_small_batches():
    result = simulate({"target": 2, "current": 8, "horizon_minutes": 20,
                       "initial_remaining_min_minutes": 60, "initial_remaining_max_minutes": 60})
    assert result["final_entries"] == 2
    assert result["departures"] == 6
    assert min(p["entries"] for p in result["points"]) >= 2


def test_rejects_mismatched_existing_session_entries():
    with pytest.raises(ValueError, match="sum"):
        simulate({"current": 20, "existing_sessions": [{"stake": "low", "entries": 2, "ends_after_minutes": 10}]})


def test_rejects_missing_spring_forward_time():
    with pytest.raises(ValueError, match="does not exist"):
        simulate({"start_local": "2026-03-29T02:30"})


def test_daily_playtime_resets_at_site_midnight(monkeypatch):
    import schedule_controller.timeline as timeline
    real_plan=timeline.plan
    observations=[]
    def observe(settings,now,target,accounts,*args,**kwargs):
        observations.append((now,accounts[0].played_seconds))
        return real_plan(settings,now,target,accounts,*args,**kwargs)
    monkeypatch.setattr(timeline,"plan",observe)
    timeline.simulate({"start_local":"2026-10-27T23:59","current":1,"target":1,"horizon_minutes":5,
        "existing_sessions":[{"stake":"low","entries":1,"started_minutes_ago":30,"ends_after_minutes":60}]}, LOW_ONLY)
    assert observations[0][1]==1800
    assert observations[1][1]==60


def test_empty_explicit_session_list_does_not_generate_fake_existing_entries():
    with pytest.raises(ValueError,match="sum"):
        simulate({"current":20,"existing_sessions":[]})
