-- Per account: the game formats it plays (ring tables and tournaments) and how many rebuys and re-entries it takes
-- per SNG / MTT. Existing accounts play all three formats; PLO5 was not joined at all before (planner: NLH, PLO4).
IF COL_LENGTH(N'scheduler.AccountPolicy',N'AllowedGames') IS NULL
    ALTER TABLE scheduler.AccountPolicy ADD AllowedGames varchar(20) NOT NULL
        CONSTRAINT DF_AccountPolicy_AllowedGames DEFAULT 'NLH,PLO4,PLO5';
IF COL_LENGTH(N'scheduler.AccountPolicy',N'MaxSngRebuys') IS NULL
    ALTER TABLE scheduler.AccountPolicy ADD MaxSngRebuys int NOT NULL CONSTRAINT DF_AccountPolicy_MaxSngRebuys DEFAULT 3;
IF COL_LENGTH(N'scheduler.AccountPolicy',N'MaxMttRebuys') IS NULL
    ALTER TABLE scheduler.AccountPolicy ADD MaxMttRebuys int NOT NULL CONSTRAINT DF_AccountPolicy_MaxMttRebuys DEFAULT 3;
IF COL_LENGTH(N'scheduler.AccountPolicy',N'MaxSngReentries') IS NULL
    ALTER TABLE scheduler.AccountPolicy ADD MaxSngReentries int NOT NULL CONSTRAINT DF_AccountPolicy_MaxSngReentries DEFAULT 1;
IF COL_LENGTH(N'scheduler.AccountPolicy',N'MaxMttReentries') IS NULL
    ALTER TABLE scheduler.AccountPolicy ADD MaxMttReentries int NOT NULL CONSTRAINT DF_AccountPolicy_MaxMttReentries DEFAULT 1;
GO
IF OBJECT_ID(N'scheduler.CK_AccountPolicy_TournamentLimits') IS NULL
    ALTER TABLE scheduler.AccountPolicy ADD CONSTRAINT CK_AccountPolicy_TournamentLimits CHECK (
        MaxSngRebuys BETWEEN 0 AND 100 AND MaxMttRebuys BETWEEN 0 AND 100
        AND MaxSngReentries BETWEEN 0 AND 100 AND MaxMttReentries BETWEEN 0 AND 100);
IF OBJECT_ID(N'scheduler.CK_AccountPolicy_AllowedGames') IS NULL
    ALTER TABLE scheduler.AccountPolicy ADD CONSTRAINT CK_AccountPolicy_AllowedGames CHECK (
        AllowedGames<>'' AND REPLACE(REPLACE(REPLACE(REPLACE(AllowedGames,'PLO4',''),'PLO5',''),'NLH',''),',','')='');
