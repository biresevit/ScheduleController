-- Additive foundation. Execute with database.py in ONE transaction.
-- No changes to PDecision procedures or existing BotAccount columns.
IF SCHEMA_ID(N'scheduler') IS NULL EXEC(N'CREATE SCHEMA scheduler');
GO
CREATE TABLE scheduler.Site (
    SiteId int IDENTITY PRIMARY KEY,
    CustomerId int NOT NULL REFERENCES dbo.Customer(CustomerId),
    Casino nvarchar(50) NOT NULL,
    Platform nvarchar(50) NOT NULL,
    TimeZone nvarchar(80) NOT NULL,
    CurrencyCode nvarchar(20) NOT NULL,
    EndpointUrl nvarchar(400) NULL,
    SchedulingEnabled bit NOT NULL DEFAULT 0,
    ConfigVersion bigint NOT NULL DEFAULT 1,
    MaxJoinsPerTick int NOT NULL DEFAULT 3 CHECK (MaxJoinsPerTick BETWEEN 0 AND 100),
    MaxLeavesPerTick int NOT NULL DEFAULT 3 CHECK (MaxLeavesPerTick BETWEEN 0 AND 100),
    MinJoinGapSeconds int NOT NULL DEFAULT 20 CHECK (MinJoinGapSeconds >= 1),
    MaxJoinGapSeconds int NOT NULL DEFAULT 60,
    HeartbeatTimeoutSeconds int NOT NULL DEFAULT 60 CHECK (HeartbeatTimeoutSeconds >= 15),
    TableFreshnessSeconds int NOT NULL DEFAULT 45 CHECK (TableFreshnessSeconds >= 1),
    SettingsRefreshSeconds int NOT NULL DEFAULT 600 CHECK (SettingsRefreshSeconds >= 1),
    NextJoinUtc datetime2(3) NULL,
    NextLeaveUtc datetime2(3) NULL,
    LastReconciledUtc datetime2(3) NULL,
    UpdatedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_SchedulerSite UNIQUE(CustomerId,Casino),
    CONSTRAINT CK_SchedulerSite_Gaps CHECK(MaxJoinGapSeconds >= MinJoinGapSeconds)
);

CREATE TABLE scheduler.StakeBand (
    SiteId int NOT NULL REFERENCES scheduler.Site(SiteId),
    Tier varchar(6) NOT NULL CHECK(Tier IN ('low','medium','high')),
    MinBigBlind decimal(19,4) NOT NULL CHECK(MinBigBlind >= 0),
    MaxBigBlind decimal(19,4) NOT NULL,
    UpperInclusive bit NOT NULL DEFAULT 0,
    TargetBasisPoints int NOT NULL CHECK(TargetBasisPoints BETWEEN 0 AND 10000),
    PRIMARY KEY(SiteId,Tier),
    CHECK(MaxBigBlind > MinBigBlind)
);

CREATE TABLE scheduler.ScheduleWindow (
    WindowId bigint IDENTITY PRIMARY KEY,
    SiteId int NOT NULL REFERENCES scheduler.Site(SiteId),
    Weekday tinyint NOT NULL CHECK(Weekday BETWEEN 0 AND 6),
    StartMinute smallint NOT NULL CHECK(StartMinute BETWEEN 0 AND 1439),
    EndMinute smallint NOT NULL CHECK(EndMinute BETWEEN 1 AND 1440),
    TargetEntries int NOT NULL CHECK(TargetEntries >= 0),
    UNIQUE(SiteId,Weekday,StartMinute),
    CHECK(StartMinute < EndMinute)
);

CREATE TABLE scheduler.MachineAssignment (
    MachineId int NOT NULL PRIMARY KEY REFERENCES dbo.CustomerMachine(MachineId),
    SiteId int NOT NULL REFERENCES scheduler.Site(SiteId),
    MaxEntries int NOT NULL DEFAULT 6 CHECK(MaxEntries BETWEEN 1 AND 10000),
    IsEnabled bit NOT NULL DEFAULT 0,
    IsReady bit NOT NULL DEFAULT 0,
    WorkerEpoch uniqueidentifier NULL,
    LastHeartbeatUtc datetime2(3) NULL,
    LastSequence bigint NOT NULL DEFAULT 0 CHECK(LastSequence >= 0),
    ControllerVersion nvarchar(40) NULL,
    DecisionUrl nvarchar(400) NULL,
    UNIQUE(SiteId,MachineId)
);

CREATE TABLE scheduler.AccountPolicy (
    BotAccountId int NOT NULL PRIMARY KEY REFERENCES dbo.BotAccount(BotAccountId),
    SiteId int NOT NULL REFERENCES scheduler.Site(SiteId),
    StakeTier varchar(6) NOT NULL,
    PlayingFrequency varchar(6) NOT NULL CHECK(PlayingFrequency IN ('low','medium','high')),
    MaxPlaytimeMinutes int NOT NULL CHECK(MaxPlaytimeMinutes BETWEEN 1 AND 1440),
    MaxTables int NOT NULL DEFAULT 6 CHECK(MaxTables BETWEEN 1 AND 100),
    PostBbChance decimal(6,5) NOT NULL CHECK(PostBbChance BETWEEN 0 AND 1),
    RunItTwiceChance decimal(6,5) NOT NULL CHECK(RunItTwiceChance BETWEEN 0 AND 1),
    BuyInPolicyJson nvarchar(max) NOT NULL CHECK(ISJSON(BuyInPolicyJson)=1),
    BreakAfterMinSeconds int NOT NULL DEFAULT 1200 CHECK(BreakAfterMinSeconds >= 1),
    BreakAfterMaxSeconds int NOT NULL DEFAULT 3600,
    BreakMinSeconds int NOT NULL DEFAULT 10 CHECK(BreakMinSeconds >= 10),
    BreakMaxSeconds int NOT NULL DEFAULT 300 CHECK(BreakMaxSeconds <= 300),
    IsSynthetic bit NOT NULL DEFAULT 0,
    IsEnabled bit NOT NULL DEFAULT 0,
    CredentialRef nvarchar(400) NULL,
    UpdatedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    UNIQUE(SiteId,BotAccountId),
    FOREIGN KEY(SiteId,StakeTier) REFERENCES scheduler.StakeBand(SiteId,Tier),
    CHECK(BreakAfterMaxSeconds >= BreakAfterMinSeconds),
    CHECK(BreakMaxSeconds >= BreakMinSeconds),
    CHECK(IsSynthetic=0 OR (IsEnabled=0 AND CredentialRef IS NULL))
);

CREATE TABLE scheduler.LobbyTable (
    SiteId int NOT NULL REFERENCES scheduler.Site(SiteId),
    TableId nvarchar(80) NOT NULL,
    TableName nvarchar(200) NOT NULL,
    GameType varchar(8) NOT NULL,
    CurrencyCode nvarchar(20) NOT NULL,
    BigBlind decimal(19,4) NOT NULL CHECK(BigBlind > 0),
    MinBuyIn decimal(19,4) NOT NULL CHECK(MinBuyIn >= 0),
    MaxBuyIn decimal(19,4) NOT NULL,
    MaxSeats smallint NOT NULL CHECK(MaxSeats > 0),
    OccupiedSeats smallint NOT NULL CHECK(OccupiedSeats >= 0),
    IsOpen bit NOT NULL,
    ObservedUtc datetime2(3) NOT NULL,
    FullSnapshotUtc datetime2(3) NULL,
    ObserverMachineId int NOT NULL,
    ObserverEpoch uniqueidentifier NOT NULL,
    SnapshotSequence bigint NOT NULL CHECK(SnapshotSequence >= 0),
    PRIMARY KEY(SiteId,TableId),
    FOREIGN KEY(SiteId,ObserverMachineId) REFERENCES scheduler.MachineAssignment(SiteId,MachineId),
    CHECK(MaxBuyIn >= MinBuyIn),
    CHECK(OccupiedSeats <= MaxSeats)
);

CREATE TABLE scheduler.TableSeat (
    SiteId int NOT NULL,
    TableId nvarchar(80) NOT NULL,
    SeatIndex smallint NOT NULL CHECK(SeatIndex >= 0),
    NativePlayerId nvarchar(80) NULL,
    Username nvarchar(100) NOT NULL,
    BotAccountId int NULL,
    Stack decimal(19,4) NOT NULL CHECK(Stack >= 0),
    CurrentBet decimal(19,4) NOT NULL CHECK(CurrentBet >= 0),
    InGame bit NOT NULL,
    SittingOut bit NOT NULL,
    HandId nvarchar(80) NULL,
    ObservedUtc datetime2(3) NOT NULL,
    PRIMARY KEY(SiteId,TableId,SeatIndex),
    FOREIGN KEY(SiteId,TableId) REFERENCES scheduler.LobbyTable(SiteId,TableId),
    FOREIGN KEY(SiteId,BotAccountId) REFERENCES scheduler.AccountPolicy(SiteId,BotAccountId)
);

CREATE TABLE scheduler.AccountLease (
    BotAccountId int NOT NULL PRIMARY KEY,
    SiteId int NOT NULL,
    MachineId int NOT NULL,
    WorkerEpoch uniqueidentifier NOT NULL,
    LeaseToken uniqueidentifier NOT NULL,
    LeaseUntilUtc datetime2(3) NOT NULL,
    LastConfirmedUtc datetime2(3) NOT NULL,
    RequiresReconciliation bit NOT NULL DEFAULT 0,
    FOREIGN KEY(SiteId,BotAccountId) REFERENCES scheduler.AccountPolicy(SiteId,BotAccountId),
    FOREIGN KEY(SiteId,MachineId) REFERENCES scheduler.MachineAssignment(SiteId,MachineId)
);

CREATE TABLE scheduler.Entry (
    EntryId uniqueidentifier NOT NULL PRIMARY KEY,
    SiteId int NOT NULL,
    BotAccountId int NOT NULL,
    MachineId int NOT NULL,
    WorkerEpoch uniqueidentifier NOT NULL,
    TableId nvarchar(80) NOT NULL,
    Status varchar(12) NOT NULL CHECK(Status IN ('reserved','joining','seated','sitting_out','draining','uncertain','closed','failed')),
    IsOpen bit NOT NULL,
    BuyInAmount decimal(19,4) NULL CHECK(BuyInAmount >= 0),
    CreatedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    SeatedUtc datetime2(3) NULL,
    ClosedUtc datetime2(3) NULL,
    LastConfirmedUtc datetime2(3) NULL,
    UNIQUE(SiteId,EntryId),
    FOREIGN KEY(SiteId,BotAccountId) REFERENCES scheduler.AccountPolicy(SiteId,BotAccountId),
    FOREIGN KEY(SiteId,MachineId) REFERENCES scheduler.MachineAssignment(SiteId,MachineId),
    FOREIGN KEY(SiteId,TableId) REFERENCES scheduler.LobbyTable(SiteId,TableId),
    CHECK((IsOpen=0 AND Status IN ('closed','failed')) OR (IsOpen=1 AND Status NOT IN ('closed','failed')))
);
CREATE UNIQUE INDEX UX_SchedulerEntry_OpenSeat ON scheduler.Entry(SiteId,BotAccountId,TableId) WHERE IsOpen=1;
CREATE INDEX IX_SchedulerEntry_Machine ON scheduler.Entry(SiteId,MachineId,IsOpen) INCLUDE(TableId,BotAccountId,Status);

CREATE TABLE scheduler.Command (
    CommandId uniqueidentifier NOT NULL PRIMARY KEY,
    SiteId int NOT NULL,
    EntryId uniqueidentifier NOT NULL,
    MachineId int NOT NULL,
    WorkerEpoch uniqueidentifier NOT NULL,
    Kind varchar(5) NOT NULL CHECK(Kind IN ('join','leave')),
    Status varchar(10) NOT NULL CHECK(Status IN ('pending','claimed','succeeded','failed','cancelled','expired','uncertain')),
    IsOpen bit NOT NULL,
    ConfigVersion bigint NOT NULL,
    PayloadJson nvarchar(max) NOT NULL CHECK(ISJSON(PayloadJson)=1),
    NotBeforeUtc datetime2(3) NOT NULL,
    ExpiresUtc datetime2(3) NOT NULL,
    ClaimToken uniqueidentifier NULL,
    ClaimUntilUtc datetime2(3) NULL,
    AttemptCount int NOT NULL DEFAULT 0 CHECK(AttemptCount >= 0),
    ResultCode nvarchar(80) NULL,
    ResultText nvarchar(1000) NULL,
    CreatedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    FOREIGN KEY(SiteId,EntryId) REFERENCES scheduler.Entry(SiteId,EntryId),
    FOREIGN KEY(SiteId,MachineId) REFERENCES scheduler.MachineAssignment(SiteId,MachineId),
    CHECK(ExpiresUtc > NotBeforeUtc),
    CHECK((IsOpen=1 AND Status IN ('pending','claimed','uncertain')) OR
          (IsOpen=0 AND Status IN ('succeeded','failed','cancelled','expired')))
);
CREATE UNIQUE INDEX UX_SchedulerCommand_Open ON scheduler.Command(EntryId,Kind) WHERE IsOpen=1;
CREATE INDEX IX_SchedulerCommand_Due ON scheduler.Command(MachineId,IsOpen,NotBeforeUtc) INCLUDE(Status,ExpiresUtc);

CREATE TABLE scheduler.Outbox (
    OutboxId bigint IDENTITY PRIMARY KEY,
    CommandId uniqueidentifier NOT NULL UNIQUE REFERENCES scheduler.Command(CommandId),
    AvailableUtc datetime2(3) NOT NULL,
    SentUtc datetime2(3) NULL,
    AttemptCount int NOT NULL DEFAULT 0 CHECK(AttemptCount >= 0),
    LastError nvarchar(1000) NULL
);
CREATE INDEX IX_SchedulerOutbox_Pending ON scheduler.Outbox(AvailableUtc) WHERE SentUtc IS NULL;

CREATE TABLE scheduler.AccountInterval (
    IntervalId uniqueidentifier NOT NULL PRIMARY KEY,
    SiteId int NOT NULL,
    BotAccountId int NOT NULL,
    MachineId int NOT NULL,
    WorkerEpoch uniqueidentifier NOT NULL,
    SequenceNumber bigint NOT NULL,
    StartUtc datetime2(3) NOT NULL,
    EndUtc datetime2(3) NOT NULL,
    EntryCount int NOT NULL CHECK(EntryCount > 0),
    UNIQUE(BotAccountId,WorkerEpoch,SequenceNumber),
    FOREIGN KEY(SiteId,BotAccountId) REFERENCES scheduler.AccountPolicy(SiteId,BotAccountId),
    FOREIGN KEY(SiteId,MachineId) REFERENCES scheduler.MachineAssignment(SiteId,MachineId),
    CHECK(EndUtc > StartUtc)
);

CREATE TABLE scheduler.AccountDay (
    SiteId int NOT NULL,
    BotAccountId int NOT NULL,
    LocalDate date NOT NULL,
    SessionSeconds bigint NOT NULL CHECK(SessionSeconds BETWEEN 0 AND 90000),
    EntrySeconds bigint NOT NULL,
    UpdatedUtc datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    PRIMARY KEY(SiteId,BotAccountId,LocalDate),
    FOREIGN KEY(SiteId,BotAccountId) REFERENCES scheduler.AccountPolicy(SiteId,BotAccountId),
    CHECK(EntrySeconds >= SessionSeconds)
);
GO
-- These checks also protect manual inserts from mixing customers/sites.
CREATE TRIGGER scheduler.AccountPolicy_CheckScope ON scheduler.AccountPolicy AFTER INSERT,UPDATE AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS(SELECT 1 FROM inserted i JOIN scheduler.Site s ON s.SiteId=i.SiteId
              JOIN dbo.BotAccount b ON b.BotAccountId=i.BotAccountId WHERE s.Casino<>b.Casino)
        THROW 51001, 'Account casino does not match scheduler site', 1;
END;
GO
CREATE TRIGGER scheduler.MachineAssignment_CheckScope ON scheduler.MachineAssignment AFTER INSERT,UPDATE AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS(SELECT 1 FROM inserted i JOIN scheduler.Site s ON s.SiteId=i.SiteId
              JOIN dbo.CustomerMachine m ON m.MachineId=i.MachineId
              WHERE s.Casino<>m.Casino OR s.CustomerId<>m.CustomerId)
        THROW 51002, 'Machine registration does not match scheduler customer/site', 1;
END;
GO
-- A single atomic configuration route prevents overlapping intervals under concurrency.
CREATE PROCEDURE scheduler.SetSchedule @SiteId int, @WindowsJson nvarchar(max) AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF ISJSON(@WindowsJson)<>1 THROW 51003, 'Invalid schedule JSON', 1;
    DECLARE @w TABLE(Weekday int NOT NULL,StartMinute int NOT NULL,EndMinute int NOT NULL,TargetEntries int NOT NULL);
    INSERT @w SELECT Weekday,StartMinute,EndMinute,TargetEntries FROM OPENJSON(@WindowsJson)
       WITH(Weekday int,StartMinute int,EndMinute int,TargetEntries int);
    IF EXISTS(SELECT 1 FROM @w WHERE Weekday NOT BETWEEN 0 AND 6 OR StartMinute<0 OR EndMinute>1440
              OR StartMinute>=EndMinute OR TargetEntries<0)
        THROW 51004, 'Invalid schedule window', 1;
    IF EXISTS(SELECT 1 FROM @w GROUP BY Weekday,StartMinute HAVING COUNT(*)>1)
        THROW 51005, 'Duplicate schedule window', 1;
    IF EXISTS(SELECT 1 FROM @w a JOIN @w b ON a.Weekday=b.Weekday
              AND a.StartMinute<b.StartMinute AND a.EndMinute>b.StartMinute)
        THROW 51006, 'Overlapping schedule windows', 1;
    BEGIN TRANSACTION;
    BEGIN TRY
        UPDATE scheduler.Site WITH(UPDLOCK,HOLDLOCK)
            SET ConfigVersion=ConfigVersion+1,UpdatedUtc=SYSUTCDATETIME() WHERE SiteId=@SiteId;
        IF @@ROWCOUNT<>1 THROW 51007, 'Unknown scheduler site', 1;
        DELETE scheduler.ScheduleWindow WHERE SiteId=@SiteId;
        INSERT scheduler.ScheduleWindow(SiteId,Weekday,StartMinute,EndMinute,TargetEntries)
            SELECT @SiteId,Weekday,StartMinute,EndMinute,TargetEntries FROM @w;
        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE()<>0 ROLLBACK;
        THROW;
    END CATCH
END;
