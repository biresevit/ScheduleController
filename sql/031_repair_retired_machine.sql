-- Pairing a customer's retired PC again (same MAC, IsActive=0) failed with "Machine belongs to another customer or
-- is inactive" and the backoffice only showed "The request could not be completed" (Simo's VMI3689543, machine 5,
-- 2026-11-07). A machine of the same customer is now reactivated, and its registration for the site's casino takes
-- the site's current platform (machine 5 still said RunYourOwnPokerSite). A machine of another customer is refused.
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
 BEGIN TRAN;
 IF @mid IS NULL BEGIN
  INSERT dbo.CustomerMachine(CustomerId,MacAddress,Casino,Platform,Description) SELECT @CustomerId,@Mac,Casino,Platform,@Host FROM scheduler.Site WHERE SiteId=@SiteId;
  SET @mid=SCOPE_IDENTITY();
 END
 ELSE IF EXISTS(SELECT 1 FROM dbo.CustomerMachine WHERE MachineId=@mid AND IsActive=0) BEGIN
  -- A retired PC of this customer comes back for this site.
  UPDATE m SET IsActive=1,Casino=s.Casino,Platform=s.Platform,Description=@Host
   FROM dbo.CustomerMachine m CROSS JOIN scheduler.Site s WHERE m.MachineId=@mid AND s.SiteId=@SiteId;
 END;
 UPDATE c SET Platform=s.Platform FROM dbo.MachineCasino c JOIN scheduler.Site s ON s.SiteId=@SiteId
  WHERE c.MachineId=@mid AND c.Casino=s.Casino AND c.Platform<>s.Platform;
 UPDATE m SET Platform=s.Platform FROM dbo.CustomerMachine m JOIN scheduler.Site s ON s.SiteId=@SiteId
  WHERE m.MachineId=@mid AND m.Casino=s.Casino AND m.Platform<>s.Platform;
 INSERT dbo.MachineCasino(MachineId,Casino,Platform) SELECT @mid,s.Casino,s.Platform FROM scheduler.Site s
  WHERE s.SiteId=@SiteId AND NOT EXISTS(SELECT 1 FROM dbo.MachineCasino c WHERE c.MachineId=@mid AND c.Casino=s.Casino);
 IF NOT EXISTS(SELECT 1 FROM scheduler.MachineCapacity WHERE MachineId=@mid) INSERT scheduler.MachineCapacity(MachineId,MaxEntries) VALUES(@mid,@Capacity);
 IF NOT EXISTS(SELECT 1 FROM scheduler.MachineAssignment WHERE MachineId=@mid AND SiteId=@SiteId)
  INSERT scheduler.MachineAssignment(MachineId,SiteId,MaxEntries,IsEnabled) SELECT @mid,@SiteId,MaxEntries,1 FROM scheduler.MachineCapacity WHERE MachineId=@mid;
 IF NOT EXISTS(SELECT 1 FROM backoffice.MachineMetadata WHERE MachineId=@mid) INSERT backoffice.MachineMetadata(MachineId,HostName) VALUES(@mid,@Host);
 IF NOT EXISTS(SELECT 1 FROM backoffice.MachineAllowedSite WHERE MachineId=@mid AND SiteId=@SiteId) INSERT backoffice.MachineAllowedSite(MachineId,SiteId) VALUES(@mid,@SiteId);
 COMMIT;
 SELECT @mid AS MachineId;
END;
