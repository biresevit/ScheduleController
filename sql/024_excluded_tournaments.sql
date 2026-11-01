-- Tournament names a site's bots must never register for, one per line. A tournament is skipped when
-- its name contains any of them, ignoring case. Evenbet's demo Shootout SNGs never deal a hand.
IF COL_LENGTH('scheduler.Site','ExcludedTournamentNames') IS NULL
    ALTER TABLE scheduler.Site ADD ExcludedTournamentNames nvarchar(2000) NOT NULL
        CONSTRAINT DF_Site_ExcludedTournamentNames DEFAULT(N'');
GO
UPDATE scheduler.Site SET ExcludedTournamentNames=N'Shootout'
WHERE Casino=N'Evenbet' AND ExcludedTournamentNames=N'';
