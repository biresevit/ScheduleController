ALTER TABLE scheduler.AccountSession ADD IsManual bit NOT NULL CONSTRAINT DF_AccountSession_IsManual DEFAULT 0;
ALTER TABLE scheduler.AccountSession ADD RequestedBy nvarchar(100) NULL;
GRANT SELECT,INSERT ON scheduler.AccountSession TO poker_backoffice_app;
GRANT SELECT ON scheduler.AccountLease TO poker_backoffice_app;
