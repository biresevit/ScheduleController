-- Close abandoned activity at the three-minute heartbeat deadline.
CREATE OR ALTER PROCEDURE scheduler.ExpireOfflineMachines
    @CustomerId int = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRANSACTION;
    DECLARE @now datetime2(3)=SYSUTCDATETIME();
    DECLARE @offline TABLE(SiteId int,MachineId int,EndedUtc datetime2(3),PRIMARY KEY(SiteId,MachineId));
    INSERT @offline
        SELECT a.SiteId,a.MachineId,DATEADD(second,180,COALESCE(a.LastHeartbeatUtc,'19000101'))
        FROM scheduler.MachineAssignment a WITH(UPDLOCK,HOLDLOCK)
        JOIN scheduler.Site s ON s.SiteId=a.SiteId
        WHERE (@CustomerId IS NULL OR s.CustomerId=@CustomerId)
          AND (a.LastHeartbeatUtc IS NULL OR a.LastHeartbeatUtc<DATEADD(second,-180,@now))
          AND (a.IsReady=1 OR EXISTS(SELECT 1 FROM scheduler.Entry e
                   WHERE e.SiteId=a.SiteId AND e.MachineId=a.MachineId AND e.IsOpen=1)
               OR EXISTS(SELECT 1 FROM scheduler.AccountSession x
                   WHERE x.SiteId=a.SiteId AND x.MachineId=a.MachineId AND x.IsOpen=1));

    -- Fence delayed updates from the expired worker before closing its journal.
    UPDATE a SET IsReady=0,WorkerEpoch=NEWID()
        FROM scheduler.MachineAssignment a JOIN @offline o ON o.SiteId=a.SiteId AND o.MachineId=a.MachineId;
    UPDATE c SET Status='expired',IsOpen=0,ResultCode='machine_offline',
        ClaimToken=NULL,ClaimUntilUtc=NULL,UpdatedUtc=@now
        FROM scheduler.Command c JOIN @offline o ON o.SiteId=c.SiteId AND o.MachineId=c.MachineId
        WHERE c.IsOpen=1;
    UPDATE e SET Status='closed',IsOpen=0,
        ClosedUtc=CASE WHEN e.CreatedUtc>o.EndedUtc THEN e.CreatedUtc
                       WHEN e.SeatedUtc>o.EndedUtc THEN e.SeatedUtc ELSE o.EndedUtc END
        FROM scheduler.Entry e JOIN @offline o ON o.SiteId=e.SiteId AND o.MachineId=e.MachineId
        WHERE e.IsOpen=1;
    UPDATE x SET IsOpen=0,EndReason='machine_offline',
        ActualEndUtc=CASE WHEN x.StartedUtc>o.EndedUtc THEN x.StartedUtc ELSE o.EndedUtc END
        FROM scheduler.AccountSession x JOIN @offline o ON o.SiteId=x.SiteId AND o.MachineId=x.MachineId
        WHERE x.IsOpen=1;
    -- Keep account ownership blocked until a returning worker reconciles live seats.
    UPDATE l SET RequiresReconciliation=1
        FROM scheduler.AccountLease l JOIN @offline o ON o.SiteId=l.SiteId AND o.MachineId=l.MachineId;
    COMMIT TRANSACTION;
END;
GO
GRANT EXECUTE ON OBJECT::scheduler.ExpireOfflineMachines TO poker_backoffice_app;
GRANT EXECUTE ON OBJECT::scheduler.ExpireOfflineMachines TO poker_scheduler_app;
