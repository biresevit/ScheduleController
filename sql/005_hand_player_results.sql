-- Parse every uploaded customer hand once, then serve reports entirely from SQL.
-- Processing is idempotent: one status row per hand and one result row per player.
IF SCHEMA_ID(N'reporting') IS NULL EXEC(N'CREATE SCHEMA reporting');
GO

IF OBJECT_ID(N'reporting.HandProcessing',N'U') IS NULL
BEGIN
    CREATE TABLE reporting.HandProcessing(
        HandHistoryId bigint NOT NULL CONSTRAINT PK_HandProcessing PRIMARY KEY
            CONSTRAINT FK_HandProcessing_History REFERENCES dbo.CustomerHandHistory(Id),
        Status varchar(12) NOT NULL CONSTRAINT DF_HandProcessing_Status DEFAULT('pending'),
        AttemptCount int NOT NULL CONSTRAINT DF_HandProcessing_Attempts DEFAULT(0),
        LeaseUntilUtc datetime2(3) NULL,
        PlayerCount smallint NULL,
        HumanCount smallint NULL,
        ParserVersion smallint NOT NULL CONSTRAINT DF_HandProcessing_Parser DEFAULT(1),
        LastError nvarchar(500) NULL,
        ProcessedUtc datetime2(3) NULL,
        UpdatedUtc datetime2(3) NOT NULL CONSTRAINT DF_HandProcessing_Updated DEFAULT SYSUTCDATETIME(),
        CONSTRAINT CK_HandProcessing_Status CHECK(Status IN ('pending','processing','completed','failed')),
        CONSTRAINT CK_HandProcessing_Counts CHECK(
            (PlayerCount IS NULL OR PlayerCount>=0) AND (HumanCount IS NULL OR HumanCount>=0))
    );
    CREATE INDEX IX_HandProcessing_Work ON reporting.HandProcessing(Status,LeaseUntilUtc,HandHistoryId)
        INCLUDE(AttemptCount,ParserVersion);
END
GO

IF OBJECT_ID(N'reporting.HandPlayerResult',N'U') IS NULL
BEGIN
    CREATE TABLE reporting.HandPlayerResult(
        HandHistoryId bigint NOT NULL
            CONSTRAINT FK_HandPlayerResult_History REFERENCES dbo.CustomerHandHistory(Id),
        PlayerName nvarchar(100) NOT NULL,
        BotAccountId int NULL CONSTRAINT FK_HandPlayerResult_Bot REFERENCES dbo.BotAccount(BotAccountId),
        IsBot bit NOT NULL,
        AmountInvested decimal(19,4) NOT NULL,
        AmountWon decimal(19,4) NOT NULL,
        NetWinnings AS CONVERT(decimal(19,4),AmountWon-AmountInvested) PERSISTED,
        CONSTRAINT PK_HandPlayerResult PRIMARY KEY(HandHistoryId,PlayerName),
        CONSTRAINT CK_HandPlayerResult_Amounts CHECK(AmountInvested>=0 AND AmountWon>=0)
    );
    CREATE INDEX IX_HandPlayerResult_BotReport ON reporting.HandPlayerResult(IsBot,BotAccountId,HandHistoryId)
        INCLUDE(PlayerName,NetWinnings);
END
GO
