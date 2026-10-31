-- Chance that a bot buys back in after losing its whole stack: at ring tables, and in SNG/MTT
-- tournaments. Each account, existing or new, starts with its own random value; the defaults
-- draw a fresh value per row (ring 0.6-1, tournaments 0.2-0.8).
IF COL_LENGTH('scheduler.AccountPolicy','RebuyRingChance') IS NULL
    ALTER TABLE scheduler.AccountPolicy ADD
        RebuyRingChance decimal(6,5) NOT NULL
            CONSTRAINT DF_AccountPolicy_RebuyRingChance DEFAULT(ROUND(0.6+0.4*RAND(CHECKSUM(NEWID())),5))
            CONSTRAINT CK_AccountPolicy_RebuyRingChance CHECK(RebuyRingChance BETWEEN 0 AND 1),
        RebuyTournamentChance decimal(6,5) NOT NULL
            CONSTRAINT DF_AccountPolicy_RebuyTournamentChance DEFAULT(ROUND(0.2+0.6*RAND(CHECKSUM(NEWID())),5))
            CONSTRAINT CK_AccountPolicy_RebuyTournamentChance CHECK(RebuyTournamentChance BETWEEN 0 AND 1);
