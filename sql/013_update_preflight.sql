-- Verify/download before draining any traffic.
DECLARE @constraint sysname=(SELECT TOP(1) name FROM sys.check_constraints WHERE parent_object_id=OBJECT_ID('scheduler.MachineUpdateTask') AND definition LIKE '%Status%');
IF @constraint IS NOT NULL BEGIN DECLARE @sql nvarchar(max)='ALTER TABLE scheduler.MachineUpdateTask DROP CONSTRAINT '+QUOTENAME(@constraint);EXEC(@sql);END;
ALTER TABLE scheduler.MachineUpdateTask ADD CONSTRAINT CK_MachineUpdateTask_Status
 CHECK(Status IN('queued','preparing','draining','stopping','installing','verifying','resuming','completed','failed','rolled_back','cancelled'));
