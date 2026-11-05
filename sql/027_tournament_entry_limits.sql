-- How many SNG and MTT entries (registered or playing) one account may hold at the same time; every account starts at 1,
-- so the same bot is not registered for several SNGs that then wait for each other to fill.
ALTER TABLE scheduler.AccountPolicy ADD
    MaxSngEntries int NOT NULL CONSTRAINT DF_AccountPolicy_MaxSngEntries DEFAULT 1
        CONSTRAINT CK_AccountPolicy_MaxSngEntries CHECK(MaxSngEntries BETWEEN 1 AND 100),
    MaxMttEntries int NOT NULL CONSTRAINT DF_AccountPolicy_MaxMttEntries DEFAULT 1
        CONSTRAINT CK_AccountPolicy_MaxMttEntries CHECK(MaxMttEntries BETWEEN 1 AND 100);
