-- Indexes for the per-tick scheduler and worker lookups that scanned their tables (2026-11-11,
-- database CPU at 100% on S0). Tables are small, so the indexes are cheap to maintain.

-- Planner: each account's previous session (OUTER APPLY ... IsOpen=0 ORDER BY ActualEndUtc DESC).
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE name='IX_AccountSession_AccountEnd' AND object_id=OBJECT_ID('scheduler.AccountSession'))
    CREATE INDEX IX_AccountSession_AccountEnd ON scheduler.AccountSession(BotAccountId,IsOpen,ActualEndUtc)
        INCLUDE(EndReason);
GO
-- Join failure limit and worker join checks: failed/succeeded joins per entry.
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE name='IX_SchedulerCommand_EntryKind' AND object_id=OBJECT_ID('scheduler.Command'))
    CREATE INDEX IX_SchedulerCommand_EntryKind ON scheduler.Command(EntryId,Kind,Status);
GO
-- Planner: latest join per PC in the last 30 minutes.
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE name='IX_SchedulerCommand_KindDue' AND object_id=OBJECT_ID('scheduler.Command'))
    CREATE INDEX IX_SchedulerCommand_KindDue ON scheduler.Command(Kind,NotBeforeUtc) INCLUDE(MachineId);
GO
-- Entries of a session (join failure limit, session close checks).
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE name='IX_SchedulerEntry_Session' AND object_id=OBJECT_ID('scheduler.Entry'))
    CREATE INDEX IX_SchedulerEntry_Session ON scheduler.Entry(SessionId) INCLUDE(IsOpen,Status);
GO
