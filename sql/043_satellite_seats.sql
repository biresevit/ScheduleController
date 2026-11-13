-- Satellite winners (2026-11-13). BluffPoker registers every ticket winner into the satellite's target MTT itself;
-- the PC worker finds that registration when the satellite ends and records it as an entry of its own.
--   * Entry.FinishPlace: the bot's final place in the tournament (SNG/MTT), as the site reported it.
--   * Entry.WonFromEntryId: for a target-MTT entry the PC adopted, the satellite entry that won the ticket.
IF COL_LENGTH(N'scheduler.Entry',N'FinishPlace') IS NULL
    ALTER TABLE scheduler.Entry ADD FinishPlace int NULL;
IF COL_LENGTH(N'scheduler.Entry',N'WonFromEntryId') IS NULL
    ALTER TABLE scheduler.Entry ADD WonFromEntryId uniqueidentifier NULL;
GO
