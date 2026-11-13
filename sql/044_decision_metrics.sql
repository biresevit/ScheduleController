-- Decision speed and missed actions (2026-11-13), written by the PC workers in batches (every 5 minutes).
--   * DecisionMetric: how long PDecision took to answer (the HTTP call only, not the human-like pause), summed per
--     PC, hour, street (1 preflop .. 4 river) and opponents still in the hand (1..9). A flush adds a row; readers sum.
--     Histogram columns count decisions by duration band.
--   * ActionTimeout: a bot that missed its turn: 'sit_out' (the table sat it out although nothing planned it),
--     'clock_acted' (BluffPoker's engine acted for it when its clock ran out).
IF OBJECT_ID(N'scheduler.DecisionMetric') IS NULL
CREATE TABLE scheduler.DecisionMetric (
    MetricId bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_DecisionMetric PRIMARY KEY,
    SiteId int NOT NULL,
    MachineId int NOT NULL,
    HourUtc datetime2(0) NOT NULL,
    Street tinyint NOT NULL,
    Opponents tinyint NOT NULL,
    Decisions int NOT NULL,
    TotalMs bigint NOT NULL,
    MaxMs int NOT NULL,
    Under250 int NOT NULL, Under500 int NOT NULL, Under1000 int NOT NULL, Under2000 int NOT NULL,
    Under5000 int NOT NULL, Over5000 int NOT NULL,
    WrittenUtc datetime2(3) NOT NULL CONSTRAINT DF_DecisionMetric_Written DEFAULT SYSUTCDATETIME()
);
GO
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE name='IX_DecisionMetric_Site_Hour')
    CREATE INDEX IX_DecisionMetric_Site_Hour ON scheduler.DecisionMetric(SiteId,HourUtc)
        INCLUDE(MachineId,Street,Opponents,Decisions,TotalMs,MaxMs,Under250,Under500,Under1000,Under2000,Under5000,Over5000);
GO
IF OBJECT_ID(N'scheduler.ActionTimeout') IS NULL
CREATE TABLE scheduler.ActionTimeout (
    TimeoutId bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_ActionTimeout PRIMARY KEY,
    SiteId int NOT NULL,
    MachineId int NOT NULL,
    BotAccountId int NULL,
    Username nvarchar(100) NULL,
    TableId nvarchar(80) NULL,
    Kind varchar(16) NOT NULL,
    OccurredUtc datetime2(3) NOT NULL
);
GO
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE name='IX_ActionTimeout_Site_Occurred')
    CREATE INDEX IX_ActionTimeout_Site_Occurred ON scheduler.ActionTimeout(SiteId,OccurredUtc) INCLUDE(MachineId,Kind);
GO
GRANT SELECT ON scheduler.DecisionMetric TO poker_backoffice_app;
GRANT SELECT ON scheduler.ActionTimeout TO poker_backoffice_app;
