DECLARE @name sysname=(SELECT TOP(1) name FROM sys.check_constraints WHERE parent_object_id=OBJECT_ID('scheduler.MachineUpdateTask') AND definition LIKE '%Status%');
IF @name IS NOT NULL BEGIN DECLARE @sql nvarchar(max)='ALTER TABLE scheduler.MachineUpdateTask DROP CONSTRAINT '+QUOTENAME(@name);EXEC(@sql);END;
ALTER TABLE scheduler.MachineUpdateTask ADD CONSTRAINT CK_MachineUpdateTask_Status
 CHECK(Status IN('queued','preparing','draining','reconciling','stopping','installing','verifying','resuming','completed','failed','rolled_back','cancelled'));
GRANT UPDATE ON scheduler.Entry TO poker_backoffice_app;
GRANT UPDATE ON scheduler.AccountSession TO poker_backoffice_app;
GRANT UPDATE ON scheduler.Command TO poker_backoffice_app;
GRANT DELETE ON scheduler.AccountLease TO poker_backoffice_app;
