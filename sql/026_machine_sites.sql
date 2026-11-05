-- One PC plays several poker sites of its customer at the same time: one assignment per (machine, site), the
-- casinos the PC plays in dbo.MachineCasino (dbo.CustomerMachine.Casino stays the first one), and one entry limit
-- for the whole PC in scheduler.MachineCapacity, shared by all of its sites.
CREATE TABLE dbo.MachineCasino(
    MachineId int NOT NULL REFERENCES dbo.CustomerMachine(MachineId),
    Casino nvarchar(50) NOT NULL,
    Platform nvarchar(50) NOT NULL,
    CONSTRAINT PK_MachineCasino PRIMARY KEY(MachineId,Casino));
GO
INSERT dbo.MachineCasino(MachineId,Casino,Platform) SELECT MachineId,Casino,Platform FROM dbo.CustomerMachine;
INSERT dbo.MachineCasino(MachineId,Casino,Platform)
    SELECT DISTINCT a.MachineId,s.Casino,s.Platform FROM scheduler.MachineAssignment a JOIN scheduler.Site s ON s.SiteId=a.SiteId
    WHERE NOT EXISTS(SELECT 1 FROM dbo.MachineCasino c WHERE c.MachineId=a.MachineId AND c.Casino=s.Casino);
GO
CREATE TRIGGER dbo.MachineCasino_InitializeCasinoSettings ON dbo.MachineCasino AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;
    INSERT dbo.CasinoSettings(CustomerId,Casino)
        SELECT DISTINCT m.CustomerId,i.Casino FROM inserted i JOIN dbo.CustomerMachine m ON m.MachineId=i.MachineId
        WHERE NOT EXISTS(SELECT 1 FROM dbo.CasinoSettings s WITH(UPDLOCK,HOLDLOCK) WHERE s.CustomerId=m.CustomerId AND s.Casino=i.Casino);
END;
GO
CREATE TABLE scheduler.MachineCapacity(
    MachineId int NOT NULL PRIMARY KEY REFERENCES dbo.CustomerMachine(MachineId),
    MaxEntries int NOT NULL CHECK(MaxEntries BETWEEN 1 AND 10000));
GO
INSERT scheduler.MachineCapacity(MachineId,MaxEntries) SELECT MachineId,MAX(MaxEntries) FROM scheduler.MachineAssignment GROUP BY MachineId;
GO
-- Every foreign key to MachineAssignment references UNIQUE(SiteId,MachineId), never the MachineId primary key.
DECLARE @pk sysname=(SELECT name FROM sys.key_constraints WHERE parent_object_id=OBJECT_ID('scheduler.MachineAssignment') AND type='PK');
EXEC(N'ALTER TABLE scheduler.MachineAssignment DROP CONSTRAINT '+@pk);
ALTER TABLE scheduler.MachineAssignment ADD CONSTRAINT PK_MachineAssignment PRIMARY KEY(MachineId,SiteId);
GO
ALTER TRIGGER scheduler.MachineAssignment_CheckScope ON scheduler.MachineAssignment AFTER INSERT,UPDATE AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS(SELECT 1 FROM inserted i JOIN scheduler.Site s ON s.SiteId=i.SiteId
              JOIN dbo.CustomerMachine m ON m.MachineId=i.MachineId
              WHERE s.CustomerId<>m.CustomerId
                 OR NOT EXISTS(SELECT 1 FROM dbo.MachineCasino c WHERE c.MachineId=i.MachineId AND c.Casino=s.Casino))
        THROW 51002, 'Machine registration does not match scheduler customer/site', 1;
END;
GO
-- A PC paired again for another site of its customer plays that site too.
CREATE OR ALTER PROCEDURE backoffice.RegisterPairedMachine
 @CustomerId int,@SiteId int,@Mac char(17),@Host nvarchar(200),@Capacity int
AS
BEGIN
 SET NOCOUNT ON;SET XACT_ABORT ON;
 IF NOT EXISTS(SELECT 1 FROM scheduler.Site s JOIN dbo.Customer c ON c.CustomerId=s.CustomerId WHERE s.SiteId=@SiteId AND c.CustomerId=@CustomerId AND c.IsActive=1)
  THROW 51130,'Invalid customer/site',1;
 DECLARE @mid int=(SELECT MachineId FROM dbo.CustomerMachine WITH(UPDLOCK,HOLDLOCK) WHERE MacAddress=@Mac);
 IF @mid IS NOT NULL AND NOT EXISTS(SELECT 1 FROM dbo.CustomerMachine WHERE MachineId=@mid AND CustomerId=@CustomerId AND IsActive=1)
  THROW 51131,'Machine belongs to another customer or is inactive',1;
 BEGIN TRAN;
 IF @mid IS NULL BEGIN
  INSERT dbo.CustomerMachine(CustomerId,MacAddress,Casino,Platform,Description) SELECT @CustomerId,@Mac,Casino,Platform,@Host FROM scheduler.Site WHERE SiteId=@SiteId;
  SET @mid=SCOPE_IDENTITY();
 END;
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
GO
-- The poker sites a PC plays (backoffice.MachineAllowedSite), set from the backoffice: a new site gets its casino and
-- an enabled assignment; a removed site's assignment is disabled and kept, its entries and sessions reference it.
CREATE OR ALTER PROCEDURE backoffice.SetMachineSites
 @CustomerId int,@MachineId int,@SiteIds nvarchar(1000),@MaxEntries int
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
  WHEN MATCHED THEN UPDATE SET MaxEntries=@MaxEntries WHEN NOT MATCHED THEN INSERT(MachineId,MaxEntries) VALUES(@MachineId,@MaxEntries);
 INSERT dbo.MachineCasino(MachineId,Casino,Platform) SELECT DISTINCT @MachineId,s.Casino,s.Platform FROM @sites x JOIN scheduler.Site s ON s.SiteId=x.SiteId
  WHERE NOT EXISTS(SELECT 1 FROM dbo.MachineCasino c WHERE c.MachineId=@MachineId AND c.Casino=s.Casino);
 INSERT scheduler.MachineAssignment(MachineId,SiteId,MaxEntries,IsEnabled) SELECT @MachineId,x.SiteId,@MaxEntries,1 FROM @sites x
  WHERE NOT EXISTS(SELECT 1 FROM scheduler.MachineAssignment a WHERE a.MachineId=@MachineId AND a.SiteId=x.SiteId);
 UPDATE a SET IsEnabled=CASE WHEN x.SiteId IS NULL THEN 0 ELSE 1 END,MaxEntries=@MaxEntries
  FROM scheduler.MachineAssignment a LEFT JOIN @sites x ON x.SiteId=a.SiteId WHERE a.MachineId=@MachineId;
 DELETE m FROM backoffice.MachineAllowedSite m WHERE m.MachineId=@MachineId AND NOT EXISTS(SELECT 1 FROM @sites x WHERE x.SiteId=m.SiteId);
 INSERT backoffice.MachineAllowedSite(MachineId,SiteId) SELECT @MachineId,x.SiteId FROM @sites x
  WHERE NOT EXISTS(SELECT 1 FROM backoffice.MachineAllowedSite m WHERE m.MachineId=@MachineId AND m.SiteId=x.SiteId);
 COMMIT;
END;
GO
-- First result set: the registration, as before. Second: every casino the PC plays.
CREATE OR ALTER PROCEDURE dbo.Machine_GetConfig
    @MacAddresses nvarchar(1000)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @machine TABLE (MachineId int, MacAddress char(17), CustomerId int, CustomerName nvarchar(100), Casino nvarchar(50), Platform nvarchar(50));

    INSERT INTO @machine
    SELECT TOP (1) m.MachineId, m.MacAddress, c.CustomerId, c.CustomerName, m.Casino, m.Platform
    FROM dbo.CustomerMachine m
    JOIN dbo.Customer c ON c.CustomerId = m.CustomerId
    WHERE m.IsActive = 1 AND c.IsActive = 1
      AND m.MacAddress IN (SELECT UPPER(LTRIM(RTRIM(value))) FROM STRING_SPLIT(@MacAddresses, ','))
    ORDER BY m.MachineId;

    UPDATE dbo.CustomerMachine SET LastSeenUtc = SYSUTCDATETIME()
    WHERE MachineId IN (SELECT MachineId FROM @machine);

    SELECT MachineId, MacAddress, CustomerId, CustomerName, Casino, Platform FROM @machine;
    SELECT c.Casino, c.Platform FROM dbo.MachineCasino c JOIN @machine m ON m.MachineId = c.MachineId ORDER BY c.Casino;
END;
GO
GRANT EXECUTE ON backoffice.SetMachineSites TO poker_backoffice_app;
GRANT SELECT ON dbo.MachineCasino TO poker_backoffice_app;
GRANT SELECT ON scheduler.MachineCapacity TO poker_backoffice_app;
GRANT SELECT ON scheduler.MachineCapacity TO poker_scheduler_app;
