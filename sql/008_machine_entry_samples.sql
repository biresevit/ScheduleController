ALTER TABLE scheduler.MachineTelemetry ADD
    ActiveEntries int NULL CHECK(ActiveEntries>=0);
