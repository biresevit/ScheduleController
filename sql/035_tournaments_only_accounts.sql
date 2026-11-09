-- AccountPolicy.AllowedGames may also be 'TOURNAMENTS': the account plays only SNGs and MTTs, never a ring table.
-- The constraint of migration 033 allowed only combinations of NLH, PLO4 and PLO5.
IF OBJECT_ID(N'scheduler.CK_AccountPolicy_AllowedGames') IS NOT NULL
    ALTER TABLE scheduler.AccountPolicy DROP CONSTRAINT CK_AccountPolicy_AllowedGames;
GO
ALTER TABLE scheduler.AccountPolicy ADD CONSTRAINT CK_AccountPolicy_AllowedGames CHECK (
    AllowedGames='TOURNAMENTS'
    OR AllowedGames<>'' AND REPLACE(REPLACE(REPLACE(REPLACE(AllowedGames,'PLO4',''),'PLO5',''),'NLH',''),',','')='');
