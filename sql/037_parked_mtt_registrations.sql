-- A future registration is durable, but is not a login/session or a playing slot.
ALTER TABLE scheduler.Entry ADD IsParked bit NOT NULL CONSTRAINT DF_Entry_IsParked DEFAULT 0;
ALTER TABLE scheduler.Site ADD NextMttLoginUtc datetime2(3) NULL;
GO
CREATE OR ALTER PROCEDURE scheduler.CancelPausedJoins @SiteId int
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @e TABLE(EntryId uniqueidentifier PRIMARY KEY);
    INSERT @e SELECT DISTINCT e.EntryId FROM scheduler.Entry e WITH(UPDLOCK,HOLDLOCK)
        JOIN scheduler.Command c ON c.EntryId=e.EntryId AND c.Kind='join' AND c.IsOpen=1 AND c.Status='pending'
        WHERE e.SiteId=@SiteId AND e.IsOpen=1 AND e.Status='reserved'
          AND COALESCE(CASE WHEN ISJSON(c.PayloadJson)=1 THEN JSON_VALUE(c.PayloadJson,'$.manual') END,'false')<>'true'
          AND COALESCE(CASE WHEN ISJSON(c.PayloadJson)=1 THEN JSON_VALUE(c.PayloadJson,'$.resume_registration') END,'false')<>'true';
    UPDATE c SET Status='cancelled',IsOpen=0,ResultCode='joins_paused',UpdatedUtc=SYSUTCDATETIME()
        FROM scheduler.Command c JOIN @e x ON x.EntryId=c.EntryId WHERE c.Kind='join' AND c.IsOpen=1;
    UPDATE scheduler.Outbox SET SentUtc=COALESCE(SentUtc,SYSUTCDATETIME()),LastError='cancelled'
        WHERE CommandId IN (SELECT c.CommandId FROM scheduler.Command c JOIN @e x ON x.EntryId=c.EntryId);
    UPDATE e SET Status='closed',IsOpen=0,ClosedUtc=SYSUTCDATETIME() FROM scheduler.Entry e JOIN @e x ON x.EntryId=e.EntryId;
    SELECT COUNT(*) AS n FROM @e;
END;
GO
