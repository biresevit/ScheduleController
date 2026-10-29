CREATE TABLE scheduler.ChatMessage(
 MessageId bigint IDENTITY PRIMARY KEY,
 SiteId int NOT NULL REFERENCES scheduler.Site(SiteId),TableId nvarchar(80) NOT NULL,
 EventKey char(64) NOT NULL,Author nvarchar(100) NOT NULL,Body nvarchar(2000) NOT NULL,
 PostedUtc datetime2(3) NULL,ReceivedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
 IsHuman bit NOT NULL,IsHistorical bit NOT NULL DEFAULT 0,
 UNIQUE(SiteId,TableId,EventKey));
CREATE INDEX IX_ChatMessage_Conversation ON scheduler.ChatMessage(SiteId,TableId,MessageId DESC);
CREATE TABLE scheduler.ChatAgent(
 SiteId int NOT NULL,MachineId int NOT NULL,WorkerEpoch uniqueidentifier NOT NULL,
 UpdatedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),PRIMARY KEY(SiteId,MachineId),
 FOREIGN KEY(SiteId,MachineId) REFERENCES scheduler.MachineAssignment(SiteId,MachineId));
CREATE TABLE scheduler.ChatCommand(
 RequestId uniqueidentifier NOT NULL PRIMARY KEY,SiteId int NOT NULL,
 EntryId uniqueidentifier NOT NULL,MachineId int NOT NULL,WorkerEpoch uniqueidentifier NOT NULL,
 Body nvarchar(2000) NOT NULL,RequestedBy nvarchar(200) NOT NULL,Provider nvarchar(30) NOT NULL,
 Status varchar(12) NOT NULL DEFAULT 'queued' CHECK(Status IN('queued','claimed','sent','failed','uncertain','expired')),
 CreatedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),UpdatedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
 ExpiresUtc datetime2(3) NOT NULL DEFAULT DATEADD(minute,2,SYSUTCDATETIME()),ResultText nvarchar(1000) NULL,
 FOREIGN KEY(SiteId,EntryId) REFERENCES scheduler.Entry(SiteId,EntryId));
CREATE INDEX IX_ChatCommand_Pending ON scheduler.ChatCommand(SiteId,MachineId,Status,CreatedUtc);
CREATE TABLE backoffice.ChatReadState(
 SiteId int NOT NULL REFERENCES scheduler.Site(SiteId),TableId nvarchar(80) NOT NULL,
 Provider nvarchar(30) NOT NULL,Subject nvarchar(200) NOT NULL,LastMessageId bigint NOT NULL DEFAULT 0,
 PRIMARY KEY(SiteId,TableId,Provider,Subject));
GO
GRANT SELECT ON scheduler.ChatMessage TO poker_backoffice_app;
GRANT SELECT ON scheduler.ChatAgent TO poker_backoffice_app;
GRANT SELECT,INSERT ON scheduler.ChatCommand TO poker_backoffice_app;
GRANT SELECT,INSERT,UPDATE ON backoffice.ChatReadState TO poker_backoffice_app;
