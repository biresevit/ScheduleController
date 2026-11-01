-- Tournament scheduling (SNG and MTT), alongside ring games.
--   * Weekly schedule windows belong to one format: RING (existing windows), SNG or MTT.
--   * Each stake tier has an optional tournament buy-in range (shared by SNG and MTT).
--   * Accounts get an add-on chance; rebuys and re-entries use RebuyTournamentChance.
--   * Tournaments are lobby rows (TableType SNG/MTT, TableId 'T' + tournament id) with a start time
--     and state, so tournament registrations are ordinary entries pointing at them.
--   * Registrations are paced per format with their own next-registration times.

IF COL_LENGTH('scheduler.ScheduleWindow','Format') IS NULL
    ALTER TABLE scheduler.ScheduleWindow ADD Format varchar(4) NOT NULL
        CONSTRAINT DF_ScheduleWindow_Format DEFAULT 'RING'
        CONSTRAINT CK_ScheduleWindow_Format CHECK(Format IN ('RING','SNG','MTT'));
GO
-- The old key (SiteId,Weekday,StartMinute) would stop an SNG window starting with a ring window.
DECLARE @old sysname = (SELECT TOP(1) kc.name FROM sys.key_constraints kc
    WHERE kc.parent_object_id=OBJECT_ID(N'scheduler.ScheduleWindow') AND kc.type='UQ'
      AND kc.name<>N'UQ_ScheduleWindow_FormatStart');
IF @old IS NOT NULL EXEC(N'ALTER TABLE scheduler.ScheduleWindow DROP CONSTRAINT ' + @old);
IF NOT EXISTS(SELECT 1 FROM sys.key_constraints WHERE name=N'UQ_ScheduleWindow_FormatStart')
    ALTER TABLE scheduler.ScheduleWindow ADD CONSTRAINT UQ_ScheduleWindow_FormatStart
        UNIQUE(SiteId,Format,Weekday,StartMinute);
GO
-- Replaces the windows of one format; the other formats keep theirs.
CREATE OR ALTER PROCEDURE scheduler.SetSchedule @SiteId int, @WindowsJson nvarchar(max), @Format varchar(4) = 'RING' AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF @Format NOT IN ('RING','SNG','MTT') THROW 51008, 'Invalid schedule format', 1;
    IF ISJSON(@WindowsJson)<>1 THROW 51003, 'Invalid schedule JSON', 1;
    DECLARE @w TABLE(Weekday int NOT NULL,StartMinute int NOT NULL,EndMinute int NOT NULL,TargetEntries int NOT NULL);
    INSERT @w SELECT Weekday,StartMinute,EndMinute,TargetEntries FROM OPENJSON(@WindowsJson)
       WITH(Weekday int,StartMinute int,EndMinute int,TargetEntries int);
    IF EXISTS(SELECT 1 FROM @w WHERE Weekday NOT BETWEEN 0 AND 6 OR StartMinute<0 OR EndMinute>1440
              OR StartMinute>=EndMinute OR TargetEntries<0)
        THROW 51004, 'Invalid schedule window', 1;
    IF EXISTS(SELECT 1 FROM @w GROUP BY Weekday,StartMinute HAVING COUNT(*)>1)
        THROW 51005, 'Duplicate schedule window', 1;
    IF EXISTS(SELECT 1 FROM @w a JOIN @w b ON a.Weekday=b.Weekday
              AND a.StartMinute<b.StartMinute AND a.EndMinute>b.StartMinute)
        THROW 51006, 'Overlapping schedule windows', 1;
    BEGIN TRANSACTION;
    BEGIN TRY
        UPDATE scheduler.Site WITH(UPDLOCK,HOLDLOCK)
            SET ConfigVersion=ConfigVersion+1,UpdatedUtc=SYSUTCDATETIME() WHERE SiteId=@SiteId;
        IF @@ROWCOUNT<>1 THROW 51007, 'Unknown scheduler site', 1;
        DELETE scheduler.ScheduleWindow WHERE SiteId=@SiteId AND Format=@Format;
        INSERT scheduler.ScheduleWindow(SiteId,Format,Weekday,StartMinute,EndMinute,TargetEntries)
            SELECT @SiteId,@Format,Weekday,StartMinute,EndMinute,TargetEntries FROM @w;
        COMMIT;
    END TRY
    BEGIN CATCH
        IF XACT_STATE()<>0 ROLLBACK;
        THROW;
    END CATCH
END;
GO
IF COL_LENGTH('scheduler.StakeBand','TournamentMinBuyIn') IS NULL
    ALTER TABLE scheduler.StakeBand ADD
        TournamentMinBuyIn decimal(19,4) NULL,
        TournamentMaxBuyIn decimal(19,4) NULL;
GO
IF NOT EXISTS(SELECT 1 FROM sys.check_constraints WHERE name=N'CK_StakeBand_TournamentBuyIn')
    ALTER TABLE scheduler.StakeBand ADD CONSTRAINT CK_StakeBand_TournamentBuyIn CHECK(
        (TournamentMinBuyIn IS NULL AND TournamentMaxBuyIn IS NULL) OR
        (TournamentMinBuyIn >= 0 AND TournamentMaxBuyIn >= TournamentMinBuyIn));
GO
-- Chance that a bot takes a tournament add-on when offered; each account draws its own value.
IF COL_LENGTH('scheduler.AccountPolicy','AddonChance') IS NULL
    ALTER TABLE scheduler.AccountPolicy ADD
        AddonChance decimal(6,5) NOT NULL
            CONSTRAINT DF_AccountPolicy_AddonChance DEFAULT(ROUND(0.2+0.6*RAND(CHECKSUM(NEWID())),5))
            CONSTRAINT CK_AccountPolicy_AddonChance CHECK(AddonChance BETWEEN 0 AND 1);
GO
IF COL_LENGTH('scheduler.LobbyTable','StartUtc') IS NULL
    ALTER TABLE scheduler.LobbyTable ADD
        StartUtc datetime2(3) NULL,
        TournamentState varchar(12) NULL
            CONSTRAINT CK_LobbyTable_TournamentState CHECK(TournamentState IN ('registering','running','completed'));
GO
-- The tournament table a registered bot currently plays (tournaments move players between tables).
IF COL_LENGTH('scheduler.Entry','PlayTableId') IS NULL
    ALTER TABLE scheduler.Entry ADD PlayTableId nvarchar(80) NULL;
GO
IF COL_LENGTH('scheduler.Site','NextSngUtc') IS NULL
    ALTER TABLE scheduler.Site ADD NextSngUtc datetime2(3) NULL, NextMttUtc datetime2(3) NULL;
GO
