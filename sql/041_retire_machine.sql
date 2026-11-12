-- Delete a machine from the Machines page. Its history (sessions, entries, hand histories, telemetry) references
-- it, so the machine is retired rather than removed: it disappears from the backoffice, the scheduler plans nothing
-- on it, its worker plays no site (machine_sites requires IsActive=1) and its updater and saved device approval stop
-- working (both require an active machine with an enabled agent). Pairing the same PC again brings it back.
CREATE OR ALTER PROCEDURE backoffice.RetireMachine
 @CustomerId int,@MachineId int
AS
BEGIN
 SET NOCOUNT ON;SET XACT_ABORT ON;
 BEGIN TRAN;
 IF NOT EXISTS(SELECT 1 FROM dbo.CustomerMachine WITH(UPDLOCK,HOLDLOCK) WHERE MachineId=@MachineId AND CustomerId=@CustomerId AND IsActive=1)
  THROW 51140,'Machine not found',1;
 IF EXISTS(SELECT 1 FROM scheduler.Entry WITH(UPDLOCK,HOLDLOCK) WHERE MachineId=@MachineId AND IsOpen=1)
  THROW 51141,'The machine still has open tables or tournament registrations. Disable it and let its bots finish first.',1;
 IF EXISTS(SELECT 1 FROM scheduler.AccountLease WITH(UPDLOCK,HOLDLOCK) WHERE MachineId=@MachineId)
  THROW 51142,'An account is still logged in on this machine. Disable it and wait until its sessions end.',1;
 IF EXISTS(SELECT 1 FROM scheduler.MachineUpdateTask WHERE MachineId=@MachineId AND StartedUtc IS NOT NULL AND CompletedUtc IS NULL)
  THROW 51143,'The machine is being updated. Wait for the update to finish.',1;
 UPDATE scheduler.MachineUpdateTask SET Status='cancelled',UpdatedUtc=SYSUTCDATETIME(),CompletedUtc=SYSUTCDATETIME(),
   Detail=N'Machine deleted'
  WHERE MachineId=@MachineId AND Status='queued' AND StartedUtc IS NULL;
 UPDATE scheduler.AccountSession SET IsOpen=0,EndReason=COALESCE(EndReason,'machine_deleted'),
   ActualEndUtc=CASE WHEN StartedUtc>SYSUTCDATETIME() THEN StartedUtc ELSE SYSUTCDATETIME() END
  WHERE MachineId=@MachineId AND IsOpen=1;
 UPDATE scheduler.MachineAssignment SET IsEnabled=0,IsReady=0 WHERE MachineId=@MachineId;
 UPDATE scheduler.MachineCapacity SET IsEnabled=0 WHERE MachineId=@MachineId;
 UPDATE scheduler.MachineUpdateAgent SET Enabled=0 WHERE MachineId=@MachineId;
 DELETE backoffice.MachineAllowedSite WHERE MachineId=@MachineId;
 UPDATE dbo.CustomerMachine SET IsActive=0 WHERE MachineId=@MachineId;
 COMMIT;
END;
GO
GRANT EXECUTE ON backoffice.RetireMachine TO poker_backoffice_app;
GO
-- As in 032, plus: a retired PC paired again plays the site it is paired for (the retirement switched it off).
CREATE OR ALTER PROCEDURE backoffice.RegisterPairedMachine
 @CustomerId int,@SiteId int,@Mac char(17),@Host nvarchar(200),@Capacity int
AS
BEGIN
 SET NOCOUNT ON;SET XACT_ABORT ON;
 IF NOT EXISTS(SELECT 1 FROM scheduler.Site s JOIN dbo.Customer c ON c.CustomerId=s.CustomerId WHERE s.SiteId=@SiteId AND c.CustomerId=@CustomerId AND c.IsActive=1)
  THROW 51130,'Invalid customer/site',1;
 DECLARE @mid int=(SELECT MachineId FROM dbo.CustomerMachine WITH(UPDLOCK,HOLDLOCK) WHERE MacAddress=@Mac);
 IF @mid IS NOT NULL AND NOT EXISTS(SELECT 1 FROM dbo.CustomerMachine WHERE MachineId=@mid AND CustomerId=@CustomerId)
  THROW 51131,'Machine belongs to another customer',1;
 DECLARE @returning bit=0;
 BEGIN TRAN;
 IF @mid IS NULL BEGIN
  INSERT dbo.CustomerMachine(CustomerId,MacAddress,Casino,Platform,Description) SELECT @CustomerId,@Mac,Casino,Platform,@Host FROM scheduler.Site WHERE SiteId=@SiteId;
  SET @mid=SCOPE_IDENTITY();
 END
 ELSE IF EXISTS(SELECT 1 FROM dbo.CustomerMachine WHERE MachineId=@mid AND IsActive=0) BEGIN
  -- A retired PC of this customer comes back for this site.
  UPDATE m SET IsActive=1,Casino=s.Casino,Platform=s.Platform,Description=@Host
   FROM dbo.CustomerMachine m CROSS JOIN scheduler.Site s WHERE m.MachineId=@mid AND s.SiteId=@SiteId;
  SET @returning=1;
 END;
 UPDATE c SET Platform=s.Platform FROM dbo.MachineCasino c JOIN scheduler.Site s ON s.SiteId=@SiteId
  WHERE c.MachineId=@mid AND c.Casino=s.Casino AND c.Platform<>s.Platform;
 UPDATE m SET Platform=s.Platform FROM dbo.CustomerMachine m JOIN scheduler.Site s ON s.SiteId=@SiteId
  WHERE m.MachineId=@mid AND m.Casino=s.Casino AND m.Platform<>s.Platform;
 INSERT dbo.MachineCasino(MachineId,Casino,Platform) SELECT @mid,s.Casino,s.Platform FROM scheduler.Site s
  WHERE s.SiteId=@SiteId AND NOT EXISTS(SELECT 1 FROM dbo.MachineCasino c WHERE c.MachineId=@mid AND c.Casino=s.Casino);
 IF NOT EXISTS(SELECT 1 FROM scheduler.MachineCapacity WHERE MachineId=@mid) INSERT scheduler.MachineCapacity(MachineId,MaxEntries) VALUES(@mid,@Capacity);
 IF @returning=1 BEGIN
  UPDATE scheduler.MachineCapacity SET IsEnabled=1,MaxEntries=@Capacity WHERE MachineId=@mid;
  UPDATE scheduler.MachineAssignment SET IsEnabled=1,MaxEntries=@Capacity WHERE MachineId=@mid AND SiteId=@SiteId;
 END;
 IF NOT EXISTS(SELECT 1 FROM scheduler.MachineAssignment WHERE MachineId=@mid AND SiteId=@SiteId)
  INSERT scheduler.MachineAssignment(MachineId,SiteId,MaxEntries,IsEnabled) SELECT @mid,@SiteId,MaxEntries,IsEnabled FROM scheduler.MachineCapacity WHERE MachineId=@mid;
 IF NOT EXISTS(SELECT 1 FROM backoffice.MachineMetadata WHERE MachineId=@mid) INSERT backoffice.MachineMetadata(MachineId,HostName) VALUES(@mid,@Host);
 IF NOT EXISTS(SELECT 1 FROM backoffice.MachineAllowedSite WHERE MachineId=@mid AND SiteId=@SiteId) INSERT backoffice.MachineAllowedSite(MachineId,SiteId) VALUES(@mid,@SiteId);
 COMMIT;
 SELECT @mid AS MachineId;
END;
