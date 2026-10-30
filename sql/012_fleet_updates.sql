CREATE TABLE scheduler.UpdateRelease(
 ReleaseId uniqueidentifier NOT NULL PRIMARY KEY,Label nvarchar(100) NOT NULL UNIQUE,
 PDecisionVersion nvarchar(100) NOT NULL,PokerControllerVersion nvarchar(100) NOT NULL,
 ManifestJson nvarchar(max) NOT NULL CHECK(ISJSON(ManifestJson)=1),Signature nvarchar(1000) NOT NULL,
 PublicKey nvarchar(2000) NOT NULL,ArtifactUrl nvarchar(2000) NOT NULL,BootstrapUrl nvarchar(2000) NULL,
 Enabled bit NOT NULL DEFAULT 1,PublishedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME());
CREATE TABLE scheduler.UpdateRollout(
 RolloutId uniqueidentifier NOT NULL PRIMARY KEY,CustomerId int NOT NULL REFERENCES dbo.Customer(CustomerId),
 SiteId int NOT NULL REFERENCES scheduler.Site(SiteId),ReleaseId uniqueidentifier NOT NULL REFERENCES scheduler.UpdateRelease(ReleaseId),
 Status varchar(16) NOT NULL CHECK(Status IN('queued','running','paused','completed','cancelled')),
 NotBeforeUtc datetime2(3) NOT NULL,CreatedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
 RequestedBy nvarchar(200) NOT NULL,UpdatedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME());
CREATE TABLE scheduler.MachineUpdateAgent(
 MachineId int NOT NULL PRIMARY KEY REFERENCES dbo.CustomerMachine(MachineId),SiteId int NOT NULL REFERENCES scheduler.Site(SiteId),
 TokenHash char(64) NOT NULL,InstanceId uniqueidentifier NULL,LastHeartbeatUtc datetime2(3) NULL,
 AgentVersion nvarchar(100) NULL,CurrentReleaseId uniqueidentifier NULL,Stage nvarchar(100) NULL,
 Detail nvarchar(1000) NULL,Enabled bit NOT NULL DEFAULT 1);
CREATE TABLE scheduler.MachineUpdateTask(
 TaskId uniqueidentifier NOT NULL PRIMARY KEY,RolloutId uniqueidentifier NOT NULL REFERENCES scheduler.UpdateRollout(RolloutId),
 SiteId int NOT NULL,MachineId int NOT NULL,Position int NOT NULL,
 Status varchar(16) NOT NULL CHECK(Status IN('queued','draining','stopping','installing','verifying','resuming','completed','failed','rolled_back','cancelled')),
 OriginalEntries int NOT NULL DEFAULT 0,RecoveryTarget int NOT NULL DEFAULT 0,
 Staged bit NOT NULL DEFAULT 0,StartedUtc datetime2(3) NULL,UpdatedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
 CompletedUtc datetime2(3) NULL,Detail nvarchar(1000) NULL,UNIQUE(RolloutId,MachineId),UNIQUE(RolloutId,Position),
 FOREIGN KEY(SiteId,MachineId) REFERENCES scheduler.MachineAssignment(SiteId,MachineId));
CREATE UNIQUE INDEX UX_UpdateTask_ActiveMachine ON scheduler.MachineUpdateTask(MachineId)
 WHERE CompletedUtc IS NULL AND StartedUtc IS NOT NULL;
ALTER TABLE scheduler.MachineAssignment ADD MaintenanceTaskId uniqueidentifier NULL REFERENCES scheduler.MachineUpdateTask(TaskId);
GO
GRANT SELECT ON scheduler.UpdateRelease TO poker_backoffice_app;
GRANT SELECT,INSERT,UPDATE ON scheduler.UpdateRollout TO poker_backoffice_app;
GRANT SELECT,INSERT,UPDATE ON scheduler.MachineUpdateTask TO poker_backoffice_app;
GRANT SELECT,INSERT,UPDATE ON scheduler.MachineUpdateAgent TO poker_backoffice_app;
GRANT SELECT ON scheduler.UpdateRelease TO poker_scheduler_app;
GRANT SELECT,UPDATE ON scheduler.UpdateRollout TO poker_scheduler_app;
GRANT SELECT,UPDATE ON scheduler.MachineUpdateTask TO poker_scheduler_app;
GRANT SELECT ON scheduler.MachineUpdateAgent TO poker_scheduler_app;
GRANT UPDATE ON scheduler.MachineAssignment TO poker_scheduler_app;
GRANT INSERT,UPDATE ON scheduler.AccountSession TO poker_scheduler_app;
