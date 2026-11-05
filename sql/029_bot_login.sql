-- The login of a bot account (BluffPoker signs in with email and password), imported with the accounts in the
-- backoffice. PCs use it instead of the account entry in their local bluffpoker.json when both are set.
IF COL_LENGTH('dbo.BotAccount','LoginEmail') IS NULL
    ALTER TABLE dbo.BotAccount ADD LoginEmail nvarchar(254) NULL, LoginPassword nvarchar(200) NULL;
