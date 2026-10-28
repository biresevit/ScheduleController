CREATE TABLE scheduler.MachineTelemetry (
    SampleId bigint IDENTITY PRIMARY KEY,
    SiteId int NOT NULL,
    MachineId int NOT NULL,
    SampledUtc datetime2(3) NOT NULL CONSTRAINT DF_MachineTelemetry_Sampled DEFAULT SYSUTCDATETIME(),
    CpuPercent decimal(5,2) NOT NULL CHECK(CpuPercent BETWEEN 0 AND 100),
    MemoryPercent decimal(5,2) NOT NULL CHECK(MemoryPercent BETWEEN 0 AND 100),
    DiskPercent decimal(5,2) NOT NULL CHECK(DiskPercent BETWEEN 0 AND 100),
    DiskFreeBytes bigint NOT NULL CHECK(DiskFreeBytes>=0),
    DiskTotalBytes bigint NOT NULL CHECK(DiskTotalBytes>0),
    FOREIGN KEY(SiteId,MachineId) REFERENCES scheduler.MachineAssignment(SiteId,MachineId)
);
CREATE INDEX IX_MachineTelemetry_Latest
    ON scheduler.MachineTelemetry(SiteId,MachineId,SampledUtc DESC)
    INCLUDE(CpuPercent,MemoryPercent,DiskPercent,DiskFreeBytes,DiskTotalBytes);
CREATE INDEX IX_SchedulerCommand_History
    ON scheduler.Command(SiteId,CreatedUtc DESC)
    INCLUDE(MachineId,EntryId,Kind,Status,UpdatedUtc,ResultCode);
CREATE INDEX IX_AccountSession_History
    ON scheduler.AccountSession(SiteId,StartedUtc DESC)
    INCLUDE(BotAccountId,MachineId,PlannedEndUtc,ActualEndUtc,IsOpen,EndReason);
