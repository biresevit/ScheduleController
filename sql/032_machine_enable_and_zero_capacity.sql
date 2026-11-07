-- A PC can be set to 0 entries, and switched off as a whole: a disabled PC joins no tables and its worker logs no
-- account in for lobby scans. Its seats that are still open finish their sessions normally.
-- scheduler.MachineCapacity.IsEnabled is the PC's switch; MachineAssignment.IsEnabled stays the effective per-site
-- flag the scheduler and the worker already check (site allowed on the PC AND the PC enabled).
DECLARE @sql nvarchar(max)=N'';
SELECT @sql+=N'ALTER TABLE '+QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id))+N'.'+QUOTENAME(OBJECT_NAME(parent_object_id))
    +N' DROP CONSTRAINT '+QUOTENAME(name)+N';'
FROM sys.check_constraints
WHERE parent_object_id IN (OBJECT_ID(N'scheduler.MachineCapacity'),OBJECT_ID(N'scheduler.MachineAssignment'))
  AND definition LIKE N'%MaxEntries%>=(1)%';
EXEC sys.sp_executesql @sql;
GO
IF OBJECT_ID(N'scheduler.CK_MachineCapacity_MaxEntries') IS NULL
    ALTER TABLE scheduler.MachineCapacity ADD CONSTRAINT CK_MachineCapacity_MaxEntries CHECK (MaxEntries>=0 AND MaxEntries<=10000);
IF OBJECT_ID(N'scheduler.CK_MachineAssignment_MaxEntries') IS NULL
    ALTER TABLE scheduler.MachineAssignment ADD CONSTRAINT CK_MachineAssignment_MaxEntries CHECK (MaxEntries>=0 AND MaxEntries<=10000);
IF COL_LENGTH(N'scheduler.MachineCapacity',N'IsEnabled') IS NULL
    ALTER TABLE scheduler.MachineCapacity ADD IsEnabled bit NOT NULL CONSTRAINT DF_MachineCapacity_IsEnabled DEFAULT 1;
GO
-- @Enabled NULL keeps the PC's current switch (callers from before this migration).
CREATE OR ALTER PROCEDURE backoffice.SetMachineSites
 @CustomerId int,@MachineId int,@SiteIds nvarchar(1000),@MaxEntries int,@Enabled bit=NULL
AS
BEGIN
 SET NOCOUNT ON;SET XACT_ABORT ON;
 IF NOT EXISTS(SELECT 1 FROM dbo.CustomerMachine WHERE MachineId=@MachineId AND CustomerId=@CustomerId AND IsActive=1)
  THROW 51133,'Machine not found',1;
 DECLARE @sites TABLE(SiteId int PRIMARY KEY);
 INSERT @sites SELECT DISTINCT TRY_CAST(value AS int) FROM STRING_SPLIT(@SiteIds,',') WHERE TRY_CAST(value AS int) IS NOT NULL;
 IF NOT EXISTS(SELECT 1 FROM @sites) THROW 51134,'A machine plays at least one poker site',1;
 IF EXISTS(SELECT 1 FROM @sites x LEFT JOIN scheduler.Site s ON s.SiteId=x.SiteId AND s.CustomerId=@CustomerId WHERE s.SiteId IS NULL)
  THROW 51135,'Poker site not found',1;
 BEGIN TRAN;
 MERGE scheduler.MachineCapacity t USING(SELECT @MachineId AS MachineId) x ON t.MachineId=x.MachineId
  WHEN MATCHED THEN UPDATE SET MaxEntries=@MaxEntries,IsEnabled=COALESCE(@Enabled,t.IsEnabled)
  WHEN NOT MATCHED THEN INSERT(MachineId,MaxEntries,IsEnabled) VALUES(@MachineId,@MaxEntries,COALESCE(@Enabled,1));
 DECLARE @on bit=(SELECT IsEnabled FROM scheduler.MachineCapacity WHERE MachineId=@MachineId);
 INSERT dbo.MachineCasino(MachineId,Casino,Platform) SELECT DISTINCT @MachineId,s.Casino,s.Platform FROM @sites x JOIN scheduler.Site s ON s.SiteId=x.SiteId
  WHERE NOT EXISTS(SELECT 1 FROM dbo.MachineCasino c WHERE c.MachineId=@MachineId AND c.Casino=s.Casino);
 INSERT scheduler.MachineAssignment(MachineId,SiteId,MaxEntries,IsEnabled) SELECT @MachineId,x.SiteId,@MaxEntries,@on FROM @sites x
  WHERE NOT EXISTS(SELECT 1 FROM scheduler.MachineAssignment a WHERE a.MachineId=@MachineId AND a.SiteId=x.SiteId);
 UPDATE a SET IsEnabled=CASE WHEN x.SiteId IS NULL THEN 0 ELSE @on END,MaxEntries=@MaxEntries
  FROM scheduler.MachineAssignment a LEFT JOIN @sites x ON x.SiteId=a.SiteId WHERE a.MachineId=@MachineId;
 DELETE m FROM backoffice.MachineAllowedSite m WHERE m.MachineId=@MachineId AND NOT EXISTS(SELECT 1 FROM @sites x WHERE x.SiteId=m.SiteId);
 INSERT backoffice.MachineAllowedSite(MachineId,SiteId) SELECT @MachineId,x.SiteId FROM @sites x
  WHERE NOT EXISTS(SELECT 1 FROM backoffice.MachineAllowedSite m WHERE m.MachineId=@MachineId AND m.SiteId=x.SiteId);
 COMMIT;
END;
GO
-- A disabled PC paired for another site stays disabled.
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
  INSERT scheduler.MachineAssignment(MachineId,SiteId,MaxEntries,IsEnabled) SELECT @mid,@SiteId,MaxEntries,IsEnabled FROM scheduler.MachineCapacity WHERE MachineId=@mid;
 IF NOT EXISTS(SELECT 1 FROM backoffice.MachineMetadata WHERE MachineId=@mid) INSERT backoffice.MachineMetadata(MachineId,HostName) VALUES(@mid,@Host);
 IF NOT EXISTS(SELECT 1 FROM backoffice.MachineAllowedSite WHERE MachineId=@mid AND SiteId=@SiteId) INSERT backoffice.MachineAllowedSite(MachineId,SiteId) VALUES(@mid,@SiteId);
 COMMIT;
 SELECT @mid AS MachineId;
END;
