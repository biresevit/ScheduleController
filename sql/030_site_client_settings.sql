-- BluffPoker's site-wide client values (client API key and browser origin), stored with the site like the bot
-- logins, so a newly paired PC needs no local bluffpoker.json. Values are set per site, not by this migration.
IF COL_LENGTH('scheduler.Site','ClientApiKey') IS NULL
    ALTER TABLE scheduler.Site ADD ClientApiKey nvarchar(400) NULL, ClientOrigin nvarchar(400) NULL;
