ALTER TABLE scheduler.MachineUpdateTask ADD IndependentInstall bit NOT NULL
 CONSTRAINT DF_MachineUpdateTask_IndependentInstall DEFAULT 0;
