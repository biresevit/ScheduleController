ALTER TABLE scheduler.UpdateRollout ADD Immediate bit NOT NULL CONSTRAINT DF_UpdateRollout_Immediate DEFAULT 0;
DECLARE @name sysname=(SELECT TOP(1) name FROM sys.check_constraints WHERE parent_object_id=OBJECT_ID('scheduler.MachineUpdateTask') AND definition LIKE '%Status%');
IF @name IS NOT NULL BEGIN DECLARE @sql nvarchar(max)='ALTER TABLE scheduler.MachineUpdateTask DROP CONSTRAINT '+QUOTENAME(@name);EXEC(@sql);END;
ALTER TABLE scheduler.MachineUpdateTask ADD CONSTRAINT CK_MachineUpdateTask_Status
 CHECK(Status IN('queued','preparing','draining','forcing','reconciling','stopping','installing','verifying','resuming','completed','failed','rolled_back','cancelled'));
