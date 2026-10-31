-- Manual control can sit the hero out and press I'm back, and can retry a turn the table refused.
DECLARE @name sysname,@sql nvarchar(400);
SELECT @name=name FROM sys.check_constraints
    WHERE parent_object_id=OBJECT_ID('scheduler.ManualTableAction') AND definition LIKE '%''raise''%';
IF @name IS NOT NULL
BEGIN
    SET @sql=N'ALTER TABLE scheduler.ManualTableAction DROP CONSTRAINT '+QUOTENAME(@name);
    EXEC(@sql);
END;
ALTER TABLE scheduler.ManualTableAction ADD CONSTRAINT CK_ManualTableAction_Action
    CHECK(Action IN ('fold','check','call','bet','raise','sit_out','sit_in'));

-- One action per turn was enforced by UNIQUE(RequestId,TurnKey), which also blocked a new choice
-- after the table refused the first. The backoffice and the worker both take the per-seat
-- ManualTable app lock before writing, and only refused actions release the turn.
SET @name=NULL;
SELECT @name=name FROM sys.key_constraints
    WHERE parent_object_id=OBJECT_ID('scheduler.ManualTableAction') AND type='UQ';
IF @name IS NOT NULL
BEGIN
    SET @sql=N'ALTER TABLE scheduler.ManualTableAction DROP CONSTRAINT '+QUOTENAME(@name);
    EXEC(@sql);
END;
IF NOT EXISTS(SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('scheduler.ManualTableAction') AND name='IX_ManualTableAction_Turn')
    CREATE INDEX IX_ManualTableAction_Turn ON scheduler.ManualTableAction(RequestId,TurnKey);
