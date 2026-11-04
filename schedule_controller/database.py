"""Transactional Azure SQL foundation migration and explicit Nenad demo seed."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re

from .rules import generate_defaults, nenad_bands

ROOT = Path(__file__).resolve().parent.parent


def connection_string() -> str:
    native = os.environ.get("SCHEDULE_SQL_CONNECTION_STRING")
    if native:
        return native  # Native ODBC connection string, including DRIVER.
    raw = os.environ.get("SqlConnectionString", "")
    if not raw:
        raise RuntimeError("Set SCHEDULE_SQL_CONNECTION_STRING (ODBC) or SqlConnectionString (ADO.NET)")
    names = {"server": "SERVER", "initial catalog": "DATABASE", "user id": "UID", "password": "PWD",
             "encrypt": "Encrypt", "trustservercertificate": "TrustServerCertificate",
             "connection timeout": "Connection Timeout"}
    # Azure Functions runs on Linux with Driver 18; this development PC has
    # Driver 17. An explicit override remains available for other hosts.
    driver = os.environ.get("SCHEDULE_ODBC_DRIVER") or (
        "ODBC Driver 17 for SQL Server" if os.name == "nt" else "ODBC Driver 18 for SQL Server"
    )
    parts = [f"DRIVER={{{driver}}}"]
    for part in raw.split(";"):
        if not part:
            continue
        key, value = part.split("=", 1)
        key = key.strip().lower()
        if key in names:
            if key in ("encrypt", "trustservercertificate"):
                value = {"true": "yes", "false": "no"}.get(value.lower(), value)
            parts.append(names[key] + "={" + value.replace("}", "}}") + "}")
    return ";".join(parts) + ";"


def migrations(cursor) -> list[str]:
    cursor.execute("""
        IF SCHEMA_ID(N'scheduler') IS NULL EXEC(N'CREATE SCHEMA scheduler');
        IF OBJECT_ID(N'scheduler.SchemaMigration',N'U') IS NULL
        CREATE TABLE scheduler.SchemaMigration(
            Version nvarchar(100) NOT NULL PRIMARY KEY,
            Sha256 char(64) NOT NULL,
            AppliedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME());
    """)
    applied = []
    for path in sorted((ROOT / "sql").glob("[0-9]*.sql")):
        sql = path.read_text(encoding="utf-8")
        checksum = hashlib.sha256(sql.replace("\r\n", "\n").encode()).hexdigest()
        previous = cursor.execute("SELECT Sha256 FROM scheduler.SchemaMigration WHERE Version=?", path.name).fetchone()
        if previous:
            if previous[0] != checksum:
                raise RuntimeError(f"Applied migration changed: {path.name}; add a new migration instead")
            continue
        for batch in re.split(r"(?im)^GO\s*$", sql):
            if batch.strip():
                cursor.execute(batch)
        cursor.execute("INSERT scheduler.SchemaMigration(Version,Sha256) VALUES(?,?)", path.name, checksum)
        applied.append(path.name)
    return applied


def seed_nenad(cursor, machine_id: int) -> dict:
    customer = cursor.execute("SELECT CustomerId FROM dbo.Customer WHERE CustomerName=N'Nenad'").fetchone()
    if customer is None:
        customer = cursor.execute("INSERT dbo.Customer(CustomerName) OUTPUT inserted.CustomerId VALUES(N'Nenad')").fetchone()
    customer_id = customer[0]
    if not cursor.execute("SELECT IsActive FROM dbo.Customer WHERE CustomerId=?", customer_id).fetchone()[0]:
        raise RuntimeError("Nenad is inactive; do not implicitly reactivate an existing customer")
    site = cursor.execute("SELECT SiteId FROM scheduler.Site WHERE CustomerId=? AND Casino=N'Evenbet'", customer_id).fetchone()
    if site is None:
        site = cursor.execute("""SET NOCOUNT ON;
            DECLARE @site TABLE(SiteId int);
            INSERT scheduler.Site(CustomerId,Casino,Platform,TimeZone,CurrencyCode,EndpointUrl)
            OUTPUT inserted.SiteId INTO @site VALUES(?,N'Evenbet',N'EvenBet',N'Europe/Budapest',N'DEMO',
            N'wss://web.evenbetpoker.com/front'); SELECT SiteId FROM @site""", customer_id).fetchone()
        for b in nenad_bands():
            cursor.execute("""INSERT scheduler.StakeBand(SiteId,Tier,MinBigBlind,MaxBigBlind,UpperInclusive,TargetBasisPoints)
                           VALUES(?,?,?,?,?,?)""", site[0], b.tier, b.minimum, b.maximum, b.upper_inclusive, b.basis_points)
    site_id = site[0]
    machine = cursor.execute("""SELECT m.CustomerId,m.Casino,m.Platform,c.CustomerName FROM dbo.CustomerMachine m
        JOIN dbo.Customer c ON c.CustomerId=m.CustomerId WHERE m.MachineId=? AND m.IsActive=1""", machine_id).fetchone()
    if not machine or machine[1:3] != ("Evenbet", "EvenBet"):
        raise RuntimeError("Seed machine must already be an active Evenbet / EvenBet registration")
    if machine[0] != customer_id and machine[3] != "Internal (Nenad dev)":
        raise RuntimeError("Refusing to reassign another customer's machine")
    prior_assignment = cursor.execute("SELECT SiteId FROM scheduler.MachineAssignment WHERE MachineId=?", machine_id).fetchone()
    if prior_assignment and prior_assignment[0] != site_id:
        raise RuntimeError("Machine already has a different scheduling assignment")
    cursor.execute("UPDATE dbo.CustomerMachine SET CustomerId=? WHERE MachineId=?", customer_id, machine_id)
    if not prior_assignment:
        cursor.execute("""INSERT scheduler.MachineAssignment(MachineId,SiteId,DecisionUrl)
                          VALUES(?,?,N'http://127.0.0.1:5005')""", machine_id, site_id)
    monica = cursor.execute("SELECT BotAccountId,Username,Strategy FROM dbo.BotAccount WHERE Casino=N'Evenbet' AND Username=N'monica'").fetchone()
    if not monica:
        raise RuntimeError("Existing Monica account not found")
    accounts = [tuple(monica)]
    for index, strategy in enumerate((1, 2, 4, 5, 7), 1):
        username = f"__schedule_demo_{index:02d}"
        account = cursor.execute("SELECT BotAccountId,Username,Strategy FROM dbo.BotAccount WHERE Casino=N'Evenbet' AND Username=?", username).fetchone()
        if account:
            policy = cursor.execute("SELECT SiteId,IsSynthetic FROM scheduler.AccountPolicy WHERE BotAccountId=?", account[0]).fetchone()
            if not policy or policy[0] != site_id or not policy[1]:
                raise RuntimeError("Synthetic name collides with an existing unmanaged account")
        else:
            account = cursor.execute("""INSERT dbo.BotAccount(Casino,Username,Strategy,IsActive)
                OUTPUT inserted.BotAccountId,inserted.Username,inserted.Strategy VALUES(N'Evenbet',?,?,0)""", username, strategy).fetchone()
        accounts.append(tuple(account))
    defaults = generate_defaults(accounts, "Nenad/Evenbet/initial-v1")
    # Current demo tables use BB 1 or 2. Keep exactly two accounts in each tier.
    if defaults[monica[0]]["stake"] != "high":
        other = next(key for key, p in defaults.items() if p["stake"] == "high")
        defaults[other]["stake"], defaults[monica[0]]["stake"] = defaults[monica[0]]["stake"], "high"
    added = 0
    for account_id, username, _ in accounts:
        existing = cursor.execute("SELECT SiteId FROM scheduler.AccountPolicy WHERE BotAccountId=?", account_id).fetchone()
        if existing:
            if existing[0] != site_id:
                raise RuntimeError("Account already belongs to another customer/site")
            continue  # Preserve every customized value on reruns.
        p = defaults[account_id]
        cursor.execute("""INSERT scheduler.AccountPolicy(BotAccountId,SiteId,StakeTier,PlayingFrequency,
            MaxPlaytimeMinutes,PostBbChance,RunItTwiceChance,BuyInPolicyJson,IsSynthetic)
            VALUES(?,?,?,?,?,?,?,?,?)""", account_id, site_id, p["stake"], p["frequency"], p["max_minutes"],
            p["post_bb_chance"], p["run_it_twice_chance"], json.dumps(p["buy_in_weights"]), account_id != monica[0])
        added += 1
    return {"customer_id": customer_id, "site_id": site_id, "machine_id": machine_id,
            "policies_inserted": added, "real_accounts": 1, "synthetic_accounts": 5,
            "scheduling_enabled": bool(cursor.execute("SELECT SchedulingEnabled FROM scheduler.Site WHERE SiteId=?", site_id).fetchone()[0])}


BLUFFPOKER_HUB = "https://hub.bluffpoker.club"
# Tables that keep a bot account's history or live state; an account referenced by any of them is never deleted.
ACCOUNT_REFERENCES = ("scheduler.AccountPolicy", "scheduler.Entry", "scheduler.AccountSession", "scheduler.AccountInterval",
                      "scheduler.AccountDay", "scheduler.TableSeat", "reporting.HandPlayerResult", "backoffice.SessionActivity")


def seed_bluffpoker(cursor, customer_name: str, machine_id: int, usernames: list[str]) -> dict:
    """BluffPoker ring scheduling for one customer: exactly these bot accounts (their logins stay in the PCs'
    bluffpoker.json), a site, stake bands, all-day RING windows, the machine assignment and account policies.
    Scheduling stays disabled until it is switched on in the backoffice. Reruns keep every customized value."""
    names = list(dict.fromkeys(u.strip() for u in usernames if u.strip()))
    if not names:
        raise RuntimeError("No BluffPoker accounts given")
    customer = cursor.execute("SELECT CustomerId,IsActive FROM dbo.Customer WHERE CustomerName=?", customer_name).fetchone()
    if not customer or not customer[1]:
        raise RuntimeError(f"Active customer {customer_name!r} not found")
    customer_id = customer[0]
    machine = cursor.execute("SELECT CustomerId,Casino FROM dbo.CustomerMachine WHERE MachineId=? AND IsActive=1", machine_id).fetchone()
    if not machine or machine[0] != customer_id or machine[1] != "BluffPoker":
        raise RuntimeError(f"Machine {machine_id} must be an active BluffPoker registration of {customer_name}")

    # Accounts: BotAccount is per casino; only the listed bots remain. One check and one delete for the rest.
    keep = ",".join("?" * len(names))
    others = f"FROM dbo.BotAccount b WHERE b.Casino=N'BluffPoker' AND b.Username NOT IN ({keep})"
    used = " UNION ALL ".join(f"SELECT TOP(1) b.Username,'{t}' {others} AND EXISTS(SELECT 1 FROM {t} x WHERE x.BotAccountId=b.BotAccountId)"
                              for t in ACCOUNT_REFERENCES)
    referenced = cursor.execute(used, *(names * len(ACCOUNT_REFERENCES))).fetchone()
    if referenced:
        raise RuntimeError(f"BluffPoker account {referenced[0]} is still referenced by {referenced[1]}; nothing deleted")
    removed = cursor.execute(f"SET NOCOUNT ON; DELETE b {others}; SELECT @@ROWCOUNT", *names).fetchone()[0]
    accounts = []
    for username in names:
        row = cursor.execute("SELECT BotAccountId,Username,Strategy FROM dbo.BotAccount WHERE Casino=N'BluffPoker' AND Username=?",
                             username).fetchone()
        if not row:
            row = cursor.execute("""INSERT dbo.BotAccount(Casino,Username,Strategy,IsActive)
                OUTPUT inserted.BotAccountId,inserted.Username,inserted.Strategy VALUES(N'BluffPoker',?,5,1)""", username).fetchone()
        accounts.append(tuple(row))

    site = cursor.execute("SELECT SiteId FROM scheduler.Site WHERE CustomerId=? AND Casino=N'BluffPoker'", customer_id).fetchone()
    if site is None:
        site = cursor.execute("""SET NOCOUNT ON;
            DECLARE @site TABLE(SiteId int);
            INSERT scheduler.Site(CustomerId,Casino,Platform,TimeZone,CurrencyCode,EndpointUrl)
            OUTPUT inserted.SiteId INTO @site VALUES(?,N'BluffPoker',N'BluffPokerPlatform',N'Europe/Rome',N'EUR',?);
            SELECT SiteId FROM @site""", customer_id, BLUFFPOKER_HUB).fetchone()
    site_id = site[0]
    if not cursor.execute("SELECT TOP(1) 1 FROM scheduler.StakeBand WHERE SiteId=?", site_id).fetchone():
        # The accounts hold about 200 chips: the 0.50/1 and 1/2 tables (80-100 big blind buy-ins) carry the target.
        for tier, low, high, inclusive, points in (("low", "0.02", "0.05", 0, 0), ("medium", "0.05", "0.5", 0, 0),
                                                   ("high", "0.5", "2", 1, 10000)):
            cursor.execute("""INSERT scheduler.StakeBand(SiteId,Tier,MinBigBlind,MaxBigBlind,UpperInclusive,TargetBasisPoints)
                              VALUES(?,?,?,?,?,?)""", site_id, tier, low, high, inclusive, points)
    windows = 0
    if not cursor.execute("SELECT TOP(1) 1 FROM scheduler.ScheduleWindow WHERE SiteId=? AND Format='RING'", site_id).fetchone():
        for weekday in range(7):
            cursor.execute("""INSERT scheduler.ScheduleWindow(SiteId,Weekday,StartMinute,EndMinute,TargetEntries,Format)
                              VALUES(?,?,0,1439,4,'RING')""", site_id, weekday)
            windows += 1
    assignment = cursor.execute("SELECT SiteId FROM scheduler.MachineAssignment WHERE MachineId=?", machine_id).fetchone()
    if assignment and assignment[0] != site_id:
        raise RuntimeError("Machine already has a different scheduling assignment")
    if not assignment:
        cursor.execute("""INSERT scheduler.MachineAssignment(MachineId,SiteId,MaxEntries,DecisionUrl)
                          VALUES(?,?,4,N'http://127.0.0.1:5005')""", machine_id, site_id)

    defaults = generate_defaults(accounts, f"{customer_name}/BluffPoker/initial-v1")
    added = 0
    for account_id, username, _ in accounts:
        policy = cursor.execute("SELECT SiteId FROM scheduler.AccountPolicy WHERE BotAccountId=?", account_id).fetchone()
        if policy:
            if policy[0] != site_id:
                raise RuntimeError(f"Account {username} already belongs to another site")
            continue
        p = defaults[account_id]
        cursor.execute("""INSERT scheduler.AccountPolicy(BotAccountId,SiteId,StakeTier,PlayingFrequency,MaxPlaytimeMinutes,
            MaxTables,PostBbChance,RunItTwiceChance,BuyInPolicyJson,IsSynthetic,IsEnabled,CredentialRef)
            VALUES(?,?,'high',?,?,2,?,?,?,0,1,?)""", account_id, site_id, p["frequency"], p["max_minutes"],
                       p["post_bb_chance"], p["run_it_twice_chance"], json.dumps({"80": 50, "100": 50}),
                       f"bluffpoker.json:{username}")
        added += 1
    return {"customer_id": customer_id, "site_id": site_id, "machine_id": machine_id,
            "accounts": [a[1] for a in accounts], "accounts_removed": removed, "ring_windows_added": windows,
            "policies_inserted": added,
            "scheduling_enabled": bool(cursor.execute("SELECT SchedulingEnabled FROM scheduler.Site WHERE SiteId=?", site_id).fetchone()[0])}


def main():
    import pyodbc
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Commit; default validates and rolls everything back")
    parser.add_argument("--seed-nenad", action="store_true")
    parser.add_argument("--machine-id", type=int)
    parser.add_argument("--seed-bluffpoker", metavar="CUSTOMER", help="Provision BluffPoker ring scheduling for this customer")
    parser.add_argument("--accounts", help="CSV with a nickname column: the only BluffPoker bots to keep (passwords are not read)")
    args = parser.parse_args()
    if (args.seed_nenad or args.seed_bluffpoker) and args.machine_id is None:
        parser.error("seeding requires explicit --machine-id")
    if args.seed_bluffpoker and not args.accounts:
        parser.error("--seed-bluffpoker requires --accounts")
    db = pyodbc.connect(connection_string(), autocommit=False, timeout=15)
    try:
        c = db.cursor()
        # ODBC autocommit=False already starts the transaction. An explicit BEGIN
        # would nest it and leave the outer transaction uncommitted on close.
        c.execute("SET XACT_ABORT ON")
        c.execute("""DECLARE @r int; EXEC @r=sys.sp_getapplock
            @Resource=N'ScheduleController:migrate',@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=10000;
            IF @r<0 THROW 51010, 'Could not acquire scheduler migration lock', 1;""")
        report = {"migrations": migrations(c)}
        if args.seed_nenad:
            report["seed"] = seed_nenad(c, args.machine_id)
        if args.seed_bluffpoker:
            import csv
            with open(args.accounts, encoding="utf-8-sig", newline="") as stream:
                names = [row["nickname"] for row in csv.DictReader(stream)]
            report["seed"] = seed_bluffpoker(c, args.seed_bluffpoker, args.machine_id, names)
        print(json.dumps(report, indent=2))
        if args.apply:
            if c.execute("SELECT @@TRANCOUNT").fetchone()[0] != 1:
                raise RuntimeError("Unexpected nested transaction; refusing ambiguous commit")
            db.commit()
            print("Committed. No tasks, queue resources or poker sessions were started.")
        else:
            db.rollback()
            print("Validated and rolled back; database unchanged.")
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
