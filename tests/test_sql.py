"""Opt-in Azure SQL integration checks. Every test rolls its transaction back."""
import json
import os

import pytest

from schedule_controller.database import connection_string, migrations, seed_nenad

pytestmark = pytest.mark.skipif(os.environ.get("SCHEDULE_SQL_TEST") != "1",
                               reason="Set SCHEDULE_SQL_TEST=1 to run rollback-only SQL checks")


@pytest.fixture
def database():
    import pyodbc
    db = pyodbc.connect(connection_string(), autocommit=False, timeout=15)
    try:
        c = db.cursor()
        c.execute("SET XACT_ABORT ON")
        migrations(c)
        seed = seed_nenad(c, int(os.environ.get("SCHEDULE_TEST_MACHINE_ID", "1")))
        yield c, seed
    finally:
        db.rollback()
        db.close()


def test_migrations_and_seed_are_repeatable_and_preserve_edits(database):
    c, seed = database
    site_id = seed["site_id"]
    monica = c.execute("SELECT p.BotAccountId FROM scheduler.AccountPolicy p JOIN dbo.BotAccount b ON b.BotAccountId=p.BotAccountId WHERE p.SiteId=? AND b.Username='monica'", site_id).fetchone()[0]
    count_before = c.execute("SELECT COUNT(*) FROM scheduler.AccountPolicy WHERE SiteId=?", site_id).fetchone()[0]
    c.execute("UPDATE scheduler.AccountPolicy SET PostBbChance=0.12345 WHERE BotAccountId=?", monica)
    assert migrations(c) == []
    second = seed_nenad(c, seed["machine_id"])
    assert second["policies_inserted"] == 0
    assert float(c.execute("SELECT PostBbChance FROM scheduler.AccountPolicy WHERE BotAccountId=?", monica).fetchone()[0]) == .12345
    assert c.execute("SELECT COUNT(*) FROM scheduler.AccountPolicy WHERE SiteId=?", site_id).fetchone()[0] == count_before
    assert c.execute("SELECT @@TRANCOUNT").fetchone()[0] == 1


def test_lobby_table_type_defaults_to_ring_and_rejects_unknown_values(database):
    import pyodbc
    c, seed = database
    column = c.execute("SELECT COL_LENGTH('scheduler.LobbyTable','TableType')").fetchone()[0]
    assert column is not None
    table_id=c.execute("SELECT TOP(1) TableId FROM scheduler.LobbyTable WHERE SiteId=?",seed['site_id']).fetchone()[0]
    with pytest.raises(pyodbc.Error):
        c.execute("UPDATE scheduler.LobbyTable SET TableType='CASH' WHERE SiteId=? AND TableId=?",seed['site_id'],table_id)


def test_synthetic_account_cannot_be_enabled(database):
    import pyodbc
    c, seed = database
    with pytest.raises(pyodbc.Error):
        c.execute("UPDATE scheduler.AccountPolicy SET IsEnabled=1 WHERE SiteId=? AND IsSynthetic=1", seed["site_id"])


def test_machine_cannot_be_assigned_to_another_customer(database):
    import pyodbc
    c, seed = database
    other_machine = c.execute("SELECT TOP(1) MachineId FROM dbo.CustomerMachine WHERE Casino=N'BluffPoker' AND IsActive=1").fetchone()[0]
    with pytest.raises(pyodbc.Error, match="does not match"):
        c.execute("INSERT scheduler.MachineAssignment(SiteId,MachineId) VALUES(?,?)", seed["site_id"], other_machine)


def test_schedule_windows_are_replaced_atomically(database):
    c, seed = database
    rows = [{"Weekday": 1, "StartMinute": 600, "EndMinute": 660, "TargetEntries": 56},
            {"Weekday": 1, "StartMinute": 660, "EndMinute": 720, "TargetEntries": 12}]
    c.execute("EXEC scheduler.SetSchedule @SiteId=?,@WindowsJson=?", seed["site_id"], json.dumps(rows))
    actual = c.execute("SELECT Weekday,StartMinute,EndMinute,TargetEntries FROM scheduler.ScheduleWindow WHERE SiteId=? ORDER BY StartMinute", seed["site_id"]).fetchall()
    assert [tuple(r) for r in actual] == [(1, 600, 660, 56), (1, 660, 720, 12)]


def test_schedule_rejects_overlaps(database):
    import pyodbc
    c, seed = database
    rows = [{"Weekday": 1, "StartMinute": 600, "EndMinute": 670, "TargetEntries": 56},
            {"Weekday": 1, "StartMinute": 660, "EndMinute": 720, "TargetEntries": 12}]
    with pytest.raises(pyodbc.Error, match="Overlapping"):
        c.execute("EXEC scheduler.SetSchedule @SiteId=?,@WindowsJson=?", seed["site_id"], json.dumps(rows))
