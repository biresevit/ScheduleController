-- Foreign-key checks from dbo.BotAccount (deleting or re-keying a bot) seek here instead of scanning the table;
-- IX_HandPlayerResult_BotReport leads with IsBot and cannot serve them.
CREATE INDEX IX_HandPlayerResult_Bot ON reporting.HandPlayerResult(BotAccountId);
