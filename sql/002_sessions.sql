-- Daily playtime preference and the duration of one login session are distinct.
ALTER TABLE scheduler.AccountPolicy ADD
    SessionMinMinutes int NOT NULL CONSTRAINT DF_AccountPolicy_SessionMin DEFAULT 60,
    SessionMaxMinutes int NOT NULL CONSTRAINT DF_AccountPolicy_SessionMax DEFAULT 240,
    SessionCooldownMinutes int NOT NULL CONSTRAINT DF_AccountPolicy_Cooldown DEFAULT 5;
GO
ALTER TABLE scheduler.AccountPolicy ADD CONSTRAINT CK_AccountPolicy_SessionDurations
    CHECK(SessionMinMinutes>=1 AND SessionMaxMinutes>=SessionMinMinutes AND SessionMaxMinutes<=1440
          AND SessionCooldownMinutes BETWEEN 0 AND 1440);

CREATE TABLE scheduler.AccountSession (
    SessionId uniqueidentifier NOT NULL PRIMARY KEY,
    SiteId int NOT NULL,
    BotAccountId int NOT NULL,
    MachineId int NOT NULL,
    WorkerEpoch uniqueidentifier NOT NULL,
    StartedUtc datetime2(3) NOT NULL,
    PlannedEndUtc datetime2(3) NOT NULL,
    ActualEndUtc datetime2(3) NULL,
    IsOpen bit NOT NULL,
    EndReason nvarchar(80) NULL,
    UNIQUE(SiteId,SessionId,BotAccountId,MachineId),
    FOREIGN KEY(SiteId,BotAccountId) REFERENCES scheduler.AccountPolicy(SiteId,BotAccountId),
    FOREIGN KEY(SiteId,MachineId) REFERENCES scheduler.MachineAssignment(SiteId,MachineId),
    CHECK(PlannedEndUtc>StartedUtc),
    CHECK(ActualEndUtc IS NULL OR ActualEndUtc>=StartedUtc),
    CHECK((IsOpen=1 AND ActualEndUtc IS NULL) OR (IsOpen=0 AND ActualEndUtc IS NOT NULL))
);
CREATE UNIQUE INDEX UX_AccountSession_Open ON scheduler.AccountSession(BotAccountId) WHERE IsOpen=1;
CREATE INDEX IX_AccountSession_End ON scheduler.AccountSession(SiteId,IsOpen,PlannedEndUtc);
ALTER TABLE scheduler.Entry ADD SessionId uniqueidentifier NULL;
GO
ALTER TABLE scheduler.Entry ADD CONSTRAINT FK_Entry_AccountSession
    FOREIGN KEY(SiteId,SessionId,BotAccountId,MachineId)
    REFERENCES scheduler.AccountSession(SiteId,SessionId,BotAccountId,MachineId);
