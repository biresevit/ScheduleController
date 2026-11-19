-- Optional, sanitized diagnostic snapshots. Deploy before enabling
-- POKERFILL_ACCOUNT_DIAGNOSTICS=1 on workers. No passwords, tokens or raw responses.
IF OBJECT_ID(N'scheduler.AccountDiagnosticSnapshot') IS NULL
BEGIN
 CREATE TABLE scheduler.AccountDiagnosticSnapshot(
    SiteId int NOT NULL, BotAccountId int NOT NULL, MachineId int NOT NULL,
    WorkerEpoch uniqueidentifier NOT NULL, ObservedUtc datetime2(3) NULL,
    AvailableNative bigint NULL, CurrencyCode varchar(8) NULL,
    Outcome varchar(40) NOT NULL,
    SnapshotSource varchar(16) NOT NULL, LastAttemptUtc datetime2(3) NOT NULL,
    WalletsJson nvarchar(2000) NULL,
    CONSTRAINT PK_AccountDiagnosticSnapshot PRIMARY KEY(SiteId,BotAccountId),
    CONSTRAINT FK_AccountDiagnosticSnapshot_Site FOREIGN KEY(SiteId) REFERENCES scheduler.Site(SiteId)
 );
END;
GO
IF COL_LENGTH(N'scheduler.Site',N'AccountDiagnosticsEnabled') IS NULL
 ALTER TABLE scheduler.Site ADD AccountDiagnosticsEnabled bit NOT NULL CONSTRAINT DF_Site_AccountDiagnosticsEnabled DEFAULT 0;
GO
GRANT SELECT,INSERT,UPDATE ON scheduler.AccountDiagnosticSnapshot TO poker_scheduler_app;
GRANT SELECT ON scheduler.AccountDiagnosticSnapshot TO poker_backoffice_app;
