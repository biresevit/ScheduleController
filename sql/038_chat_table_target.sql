-- A queued tournament reply belongs to the requested table, even if the bot moves.
ALTER TABLE scheduler.ChatCommand ADD TableId nvarchar(100) NULL;
GO
UPDATE c SET TableId=COALESCE(e.PlayTableId,e.TableId)
FROM scheduler.ChatCommand c JOIN scheduler.Entry e ON e.EntryId=c.EntryId
WHERE c.TableId IS NULL AND c.Status IN ('queued','claimed');
