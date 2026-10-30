ALTER TABLE scheduler.MachineUpdateAgent ADD AppsHealthy bit NOT NULL CONSTRAINT DF_MachineUpdateAgent_AppsHealthy DEFAULT 0;
