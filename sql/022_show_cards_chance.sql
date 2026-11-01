-- Chance that a bot shows its cards after winning a hand uncontested, when the hand qualifies
-- (NLH: two pair or better, PLO: three of a kind or better, or a won bluff). Strong profiles
-- (Strategy 5-8) rarely show (2-10%); weak profiles vary widely (5-90%).
IF COL_LENGTH('scheduler.AccountPolicy','ShowCardsChance') IS NULL
    ALTER TABLE scheduler.AccountPolicy ADD
        ShowCardsChance decimal(6,5) NOT NULL
            CONSTRAINT DF_AccountPolicy_ShowCardsChance DEFAULT(ROUND(0.05+0.85*RAND(CHECKSUM(NEWID())),5))
            CONSTRAINT CK_AccountPolicy_ShowCardsChance CHECK(ShowCardsChance BETWEEN 0 AND 1);
GO
UPDATE p SET ShowCardsChance=ROUND(CASE WHEN b.Strategy>=5 THEN 0.02+0.08*RAND(CHECKSUM(NEWID()))
                                        ELSE 0.05+0.85*RAND(CHECKSUM(NEWID())) END,5)
FROM scheduler.AccountPolicy p JOIN dbo.BotAccount b ON b.BotAccountId=p.BotAccountId;
