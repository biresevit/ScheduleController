"""Read-only inspection of the Nenad/Evenbet foundation; excludes secret references."""
import json

from .database import connection_string


def main():
    import pyodbc
    db = pyodbc.connect(connection_string(), autocommit=True, timeout=15)
    try:
        c = db.cursor()
        site = c.execute("""SELECT s.SiteId,s.CustomerId,c.CustomerName,s.Casino,s.TimeZone,s.CurrencyCode,s.SchedulingEnabled
            FROM scheduler.Site s JOIN dbo.Customer c ON c.CustomerId=s.CustomerId
            WHERE c.CustomerName=N'Nenad' AND s.Casino=N'Evenbet'""").fetchone()
        if not site:
            raise RuntimeError("Nenad / Evenbet scheduler site is not provisioned")
        def rows(query):
            c.execute(query, site[0])
            names = [column[0] for column in c.description]
            return [dict(zip(names, row)) for row in c.fetchall()]
        report = {"site": dict(zip(("site_id", "customer_id", "customer", "casino", "timezone", "currency", "enabled"), site)),
            "bands": rows("SELECT Tier,MinBigBlind,MaxBigBlind,UpperInclusive,TargetBasisPoints FROM scheduler.StakeBand WHERE SiteId=? ORDER BY MinBigBlind"),
            "accounts": rows("""SELECT b.Username,b.Strategy,b.IsActive,p.StakeTier,p.PlayingFrequency,p.MaxPlaytimeMinutes,
                p.PostBbChance,p.RunItTwiceChance,p.IsSynthetic,p.IsEnabled FROM scheduler.AccountPolicy p
                JOIN dbo.BotAccount b ON b.BotAccountId=p.BotAccountId WHERE p.SiteId=? ORDER BY b.Username"""),
            "machines": rows("SELECT MachineId,MaxEntries,IsEnabled,IsReady FROM scheduler.MachineAssignment WHERE SiteId=?"),
            "schedule": rows("SELECT Weekday,StartMinute,EndMinute,TargetEntries FROM scheduler.ScheduleWindow WHERE SiteId=?"),
            "commands": rows("SELECT COUNT(*) AS Count FROM scheduler.Command WHERE SiteId=?")}
        print(json.dumps(report, default=str, indent=2))
    finally:
        db.close()


if __name__ == "__main__":
    main()
