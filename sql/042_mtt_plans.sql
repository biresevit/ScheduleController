-- MTT registration plans replace the MTT weekly schedule (2026-11-12).
--   * scheduler.MttPlan: per tournament (keyed by its name without the date, so one plan covers every recurrence):
--     how many bots register, how many of them rebuy / re-enter and take the add-on, the share registered before
--     the start (the rest registers during late registration) and how early registration starts.
--   * LobbyTable.TournamentInfo: what the lobby says about the tournament (JSON: rebuy, reentry, addon, late_reg,
--     late_reg_until), when the site publishes it (BluffPoker does; EvenBet's lobby does not).
--   * Entry.TournamentRebuy / TournamentAddon: the plan's choice for this registration (NULL: the account's chances).
IF OBJECT_ID(N'scheduler.MttPlan') IS NULL
CREATE TABLE scheduler.MttPlan (
    SiteId int NOT NULL REFERENCES scheduler.Site(SiteId),
    SeriesName nvarchar(200) NOT NULL,
    TotalBots int NOT NULL CONSTRAINT CK_MttPlan_TotalBots CHECK(TotalBots BETWEEN 0 AND 1000),
    RebuyBots int NOT NULL CONSTRAINT DF_MttPlan_RebuyBots DEFAULT 0,
    AddonBots int NOT NULL CONSTRAINT DF_MttPlan_AddonBots DEFAULT 0,
    EarlyPercent int NOT NULL CONSTRAINT DF_MttPlan_EarlyPercent DEFAULT 40
        CONSTRAINT CK_MttPlan_EarlyPercent CHECK(EarlyPercent BETWEEN 0 AND 100),
    EarlyHours decimal(5,2) NOT NULL CONSTRAINT DF_MttPlan_EarlyHours DEFAULT 2
        CONSTRAINT CK_MttPlan_EarlyHours CHECK(EarlyHours > 0 AND EarlyHours <= 72),
    UpdatedUtc datetime2(3) NOT NULL CONSTRAINT DF_MttPlan_UpdatedUtc DEFAULT SYSUTCDATETIME(),
    UpdatedBy nvarchar(200) NULL,
    CONSTRAINT PK_MttPlan PRIMARY KEY(SiteId,SeriesName),
    CONSTRAINT CK_MttPlan_Counts CHECK(RebuyBots BETWEEN 0 AND TotalBots AND AddonBots BETWEEN 0 AND TotalBots)
);
GO
IF COL_LENGTH(N'scheduler.LobbyTable',N'TournamentInfo') IS NULL
    ALTER TABLE scheduler.LobbyTable ADD TournamentInfo nvarchar(2000) NULL;
IF COL_LENGTH(N'scheduler.Entry',N'TournamentRebuy') IS NULL
    ALTER TABLE scheduler.Entry ADD TournamentRebuy bit NULL, TournamentAddon bit NULL;
GO
GRANT SELECT,INSERT,UPDATE,DELETE ON scheduler.MttPlan TO poker_backoffice_app;
GRANT SELECT ON scheduler.MttPlan TO poker_scheduler_app;
