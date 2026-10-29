"""Durable chat inbox/outbox for the fenced local worker."""
from datetime import timezone


class ChatRepository:
    def _chat_owner(self,db):
        row=db.one("SELECT WorkerEpoch,IsReady FROM scheduler.MachineAssignment WITH(UPDLOCK,HOLDLOCK) WHERE SiteId=1 AND MachineId=1")
        if not row or not row["IsReady"] or str(row["WorkerEpoch"]).lower()!=str(self.epoch).lower():
            raise RuntimeError("Chat worker epoch lost")

    def chat_exchange(self,messages):
        with self.transaction() as db:
            self._chat_owner(db)
            db.execute("""UPDATE scheduler.ChatAgent SET WorkerEpoch=?,UpdatedUtc=SYSUTCDATETIME() WHERE SiteId=1 AND MachineId=1;
                IF @@ROWCOUNT=0 INSERT scheduler.ChatAgent(SiteId,MachineId,WorkerEpoch) VALUES(1,1,?)""",self.epoch,self.epoch)
            for m in messages:
                db.execute("""IF NOT EXISTS(SELECT 1 FROM scheduler.ChatMessage WITH(UPDLOCK,HOLDLOCK)
                        WHERE SiteId=1 AND TableId=? AND EventKey=?)
                    INSERT scheduler.ChatMessage(SiteId,TableId,EventKey,Author,Body,PostedUtc,ReceivedUtc,IsHuman,IsHistorical)
                    SELECT 1,?,?,?,?,?,?,CASE WHEN EXISTS(
                        SELECT 1 FROM dbo.BotAccount b JOIN scheduler.Site s ON s.Casino=b.Casino
                        WHERE s.SiteId=1 AND b.Username=?) OR EXISTS(
                        SELECT 1 FROM scheduler.TableSeat WHERE SiteId=1 AND TableId=? AND Username=? AND BotAccountId IS NOT NULL)
                        THEN 0 ELSE 1 END,?""",
                    m["table_id"],m["event_key"],m["table_id"],m["event_key"],m["author"],m["body"],
                    m["posted"].astimezone(timezone.utc).replace(tzinfo=None) if m["posted"] else None,
                    m["received"].astimezone(timezone.utc).replace(tzinfo=None),m["author"],m["table_id"],m["author"],m["historical"])
            db.execute("""UPDATE scheduler.ChatCommand SET Status='expired',ResultText='The message expired before it could be sent.',UpdatedUtc=SYSUTCDATETIME()
                WHERE SiteId=1 AND MachineId=1 AND Status='queued' AND (ExpiresUtc<=SYSUTCDATETIME() OR WorkerEpoch<>?
                    OR NOT EXISTS(SELECT 1 FROM scheduler.Entry e WHERE e.EntryId=ChatCommand.EntryId AND e.IsOpen=1));
                UPDATE scheduler.ChatCommand SET Status='uncertain',ResultText='Sending was interrupted. Check the table chat before trying again.',UpdatedUtc=SYSUTCDATETIME()
                WHERE SiteId=1 AND MachineId=1 AND Status='claimed' AND (WorkerEpoch<>? OR UpdatedUtc<DATEADD(second,-30,SYSUTCDATETIME()))""",self.epoch,self.epoch)

    def claim_chat(self):
        with self.transaction() as db:
            self._chat_owner(db)
            row=db.one("""SELECT TOP(1) c.RequestId,c.Body,e.TableId,e.BotAccountId,b.Username
                FROM scheduler.ChatCommand c WITH(UPDLOCK,READPAST,ROWLOCK)
                JOIN scheduler.Entry e ON e.EntryId=c.EntryId JOIN dbo.BotAccount b ON b.BotAccountId=e.BotAccountId
                WHERE c.SiteId=1 AND c.MachineId=1 AND c.WorkerEpoch=? AND c.Status='queued'
                  AND c.ExpiresUtc>SYSUTCDATETIME() AND e.WorkerEpoch=? AND e.IsOpen=1
                  AND e.Status IN ('seated','sitting_out','draining')
                ORDER BY c.CreatedUtc""",self.epoch,self.epoch)
            if row:
                db.execute("UPDATE scheduler.ChatCommand SET Status='claimed',UpdatedUtc=SYSUTCDATETIME() WHERE RequestId=?",row["RequestId"])
            return row

    def finish_chat(self,request_id,status,detail):
        if status not in ('sent','failed','uncertain'): raise ValueError('Invalid chat outcome')
        with self.transaction() as db:
            db.execute("""UPDATE scheduler.ChatCommand SET Status=?,ResultText=?,UpdatedUtc=SYSUTCDATETIME()
                WHERE RequestId=? AND WorkerEpoch=? AND Status='claimed'""",status,detail,request_id,self.epoch)
