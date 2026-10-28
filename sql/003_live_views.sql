-- Low-cost lobby polling and explicitly requested, short-lived table diagnostics.
ALTER TABLE scheduler.Site ADD LobbyRefreshSeconds int NOT NULL
    CONSTRAINT DF_SchedulerSite_LobbyRefresh DEFAULT 15;
GO
ALTER TABLE scheduler.Site ADD CONSTRAINT CK_SchedulerSite_LobbyRefresh
    CHECK(LobbyRefreshSeconds BETWEEN 5 AND 3600);

CREATE TABLE scheduler.LiveViewRequest (
    RequestId uniqueidentifier NOT NULL PRIMARY KEY,
    SiteId int NOT NULL,
    TableId nvarchar(80) NOT NULL,
    BotAccountId int NOT NULL,
    MachineId int NOT NULL,
    RequestedBy nvarchar(200) NOT NULL,
    RequestedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    ExpiresUtc datetime2(3) NOT NULL,
    LastSnapshotUtc datetime2(3) NULL,
    Status varchar(10) NOT NULL CHECK(Status IN ('requested','active','expired','failed')),
    ErrorText nvarchar(400) NULL,
    FOREIGN KEY(SiteId,TableId) REFERENCES scheduler.LobbyTable(SiteId,TableId),
    FOREIGN KEY(SiteId,BotAccountId) REFERENCES scheduler.AccountPolicy(SiteId,BotAccountId),
    FOREIGN KEY(SiteId,MachineId) REFERENCES scheduler.MachineAssignment(SiteId,MachineId),
    CHECK(ExpiresUtc>RequestedUtc)
);
CREATE INDEX IX_LiveViewRequest_Worker ON scheduler.LiveViewRequest(MachineId,Status,ExpiresUtc)
    INCLUDE(SiteId,TableId,BotAccountId);

CREATE TABLE scheduler.LiveTableSnapshot (
    RequestId uniqueidentifier NOT NULL PRIMARY KEY REFERENCES scheduler.LiveViewRequest(RequestId),
    SequenceNumber bigint NOT NULL CHECK(SequenceNumber>=1),
    CapturedUtc datetime2(3) NOT NULL,
    HandId nvarchar(80) NULL,
    PayloadJson nvarchar(max) NOT NULL CHECK(ISJSON(PayloadJson)=1)
);
