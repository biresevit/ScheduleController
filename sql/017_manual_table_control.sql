CREATE TABLE scheduler.ManualTableControl(
 SiteId int NOT NULL,BotAccountId int NOT NULL,TableId nvarchar(80) NOT NULL,
 MachineId int NOT NULL,WorkerEpoch uniqueidentifier NOT NULL,
 RequestId uniqueidentifier NOT NULL REFERENCES scheduler.LiveViewRequest(RequestId),
 Enabled bit NOT NULL DEFAULT 0,UpdatedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
 PRIMARY KEY(SiteId,BotAccountId,TableId));
CREATE TABLE scheduler.TableActionClaim(
 SiteId int NOT NULL,BotAccountId int NOT NULL,TableId nvarchar(80) NOT NULL,
 TurnKey char(64) NOT NULL,Source varchar(10) NOT NULL,
 CreatedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
 PRIMARY KEY(SiteId,BotAccountId,TableId,TurnKey));
CREATE TABLE scheduler.ManualTableAction(
 ActionId uniqueidentifier NOT NULL PRIMARY KEY,
 RequestId uniqueidentifier NOT NULL REFERENCES scheduler.LiveViewRequest(RequestId),
 SiteId int NOT NULL,BotAccountId int NOT NULL,TableId nvarchar(80) NOT NULL,
 MachineId int NOT NULL,WorkerEpoch uniqueidentifier NOT NULL,TurnKey char(64) NOT NULL,
 Action varchar(10) NOT NULL CHECK(Action IN ('fold','check','call','bet','raise')),
 TargetMinor bigint NULL,
 Status varchar(12) NOT NULL DEFAULT 'queued' CHECK(Status IN ('queued','submitted','sent','failed','uncertain','expired')),
 ResultText nvarchar(500) NULL,CreatedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
 UpdatedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
 UNIQUE(RequestId,TurnKey));
CREATE INDEX IX_ManualAction_Worker ON scheduler.ManualTableAction(MachineId,Status,CreatedUtc);
GO
GRANT SELECT,INSERT,UPDATE ON scheduler.ManualTableControl TO poker_backoffice_app;
GRANT SELECT ON scheduler.TableActionClaim TO poker_backoffice_app;
GRANT SELECT,INSERT ON scheduler.ManualTableAction TO poker_backoffice_app;
