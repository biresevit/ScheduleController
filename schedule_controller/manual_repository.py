"""Fence manual/automatic action claims using the same lock as the website."""


from .sql_retry import retry_deadlocks
class ManualRepository:
    def _manual_lock(self,db,aid,tid):
        result=db.one("""DECLARE @r int;EXEC @r=sys.sp_getapplock @Resource=?,@LockMode='Exclusive',
            @LockOwner='Transaction',@LockTimeout=5000;SELECT @r AS value""",f'ManualTable:{self.site_id}:{aid}:{tid}')
        if not result or result['value']<0:raise RuntimeError('Manual table lock unavailable')
        machine=db.one('SELECT WorkerEpoch FROM scheduler.MachineAssignment WHERE SiteId=:site AND MachineId=:machine')
        if not machine or str(machine['WorkerEpoch']).lower()!=str(self.epoch).lower():raise RuntimeError('Worker epoch lost')

    def _manual_mode(self,db,aid,tid):
        return db.one("""SELECT c.RequestId,c.Enabled FROM scheduler.ManualTableControl c
            JOIN scheduler.LiveViewRequest r ON r.RequestId=c.RequestId
            WHERE c.SiteId=:site AND c.BotAccountId=? AND c.TableId=? AND c.MachineId=:machine
                AND c.WorkerEpoch=? AND c.Enabled=1 AND r.Status IN ('requested','active')
                AND r.ExpiresUtc>SYSUTCDATETIME()
                AND (r.TableId=c.TableId OR EXISTS(SELECT 1 FROM scheduler.Entry e
                    JOIN scheduler.LiveTableSnapshot s ON s.RequestId=r.RequestId
                    WHERE e.SiteId=c.SiteId AND e.MachineId=c.MachineId AND e.BotAccountId=c.BotAccountId
                      AND e.TableId=r.TableId AND e.PlayTableId=c.TableId AND e.IsOpen=1
                      AND e.WorkerEpoch=c.WorkerEpoch AND JSON_VALUE(s.PayloadJson,'$.table_id')=c.TableId))""",aid,str(tid),self.epoch)

    @retry_deadlocks
    def end_live_table_views(self,aid,tid):
        """A moved/closed tournament table never transfers a manual lease to its successor."""
        with self.transaction() as db:
            self._manual_lock(db,aid,tid)
            self._end_live_table_views(db,aid,tid)

    def _end_live_table_views(self,db,aid,tid):
        detail='The bot left or changed this tournament table. Open a new live view to continue.'
        db.execute("""UPDATE r SET Status='failed',ErrorText=? FROM scheduler.LiveViewRequest r
            JOIN scheduler.LiveTableSnapshot s ON s.RequestId=r.RequestId
            WHERE r.SiteId=:site AND r.MachineId=:machine AND r.BotAccountId=?
              AND LEFT(r.TableId,1)='T' AND JSON_VALUE(s.PayloadJson,'$.table_id')=?
              AND r.Status IN ('requested','active')""",detail,aid,str(tid))
        db.execute("""UPDATE c SET Enabled=0,UpdatedUtc=SYSUTCDATETIME() FROM scheduler.ManualTableControl c
            JOIN scheduler.LiveViewRequest r ON r.RequestId=c.RequestId
            WHERE c.SiteId=:site AND c.MachineId=:machine AND c.BotAccountId=? AND c.TableId=?
              AND r.Status='failed'""",aid,str(tid))
        db.execute("""UPDATE x SET Status='expired',ResultText=?,UpdatedUtc=SYSUTCDATETIME()
            FROM scheduler.ManualTableAction x JOIN scheduler.LiveViewRequest r ON r.RequestId=x.RequestId
            WHERE x.SiteId=:site AND x.MachineId=:machine AND x.BotAccountId=? AND x.TableId=?
              AND x.Status='queued' AND r.Status='failed'""",detail,aid,str(tid))

    @retry_deadlocks
    def manual_offer(self,aid,tid,key):
        with self.transaction() as db:
            self._manual_lock(db,aid,tid);mode=self._manual_mode(db,aid,tid)
            db.execute("""UPDATE scheduler.ManualTableAction SET Status='expired',ResultText='The turn or manual-control lease ended.',UpdatedUtc=SYSUTCDATETIME()
                WHERE SiteId=:site AND MachineId=:machine AND BotAccountId=? AND TableId=? AND Status='queued'
                  AND Action NOT IN ('sit_out','sit_in')
                  AND (TurnKey<>? OR RequestId<>? OR WorkerEpoch<>?)""",aid,str(tid),key,mode['RequestId'] if mode else '00000000-0000-0000-0000-000000000000',self.epoch)
            if not mode:return False,None
            command=db.one("SELECT TOP(1) * FROM scheduler.ManualTableAction WHERE RequestId=? AND TurnKey=? AND Status='queued' AND Action NOT IN ('sit_out','sit_in') ORDER BY CreatedUtc",mode['RequestId'],key)
            return True,command

    @retry_deadlocks
    def manual_enabled(self,aid,tid):
        with self.transaction() as db:
            return bool(self._manual_mode(db,aid,tid))

    @retry_deadlocks
    def claim_table_command(self,aid,tid):
        """Claim the oldest queued sit-out / I'm-back command; they are not tied to a turn."""
        with self.transaction() as db:
            self._manual_lock(db,aid,tid);mode=self._manual_mode(db,aid,tid)
            stale="""UPDATE scheduler.ManualTableAction SET Status='expired',ResultText='Manual control ended before the command was sent.',UpdatedUtc=SYSUTCDATETIME()
                WHERE SiteId=:site AND MachineId=:machine AND BotAccountId=? AND TableId=? AND Status='queued' AND Action IN ('sit_out','sit_in')"""
            if not mode:
                db.execute(stale,aid,str(tid));return None
            db.execute(stale+" AND (RequestId<>? OR WorkerEpoch<>?)",aid,str(tid),mode['RequestId'],self.epoch)
            command=db.one("""SELECT TOP(1) * FROM scheduler.ManualTableAction WHERE RequestId=? AND Status='queued'
                AND Action IN ('sit_out','sit_in') ORDER BY CreatedUtc""",mode['RequestId'])
            if command:
                db.execute("UPDATE scheduler.ManualTableAction SET Status='submitted',UpdatedUtc=SYSUTCDATETIME() WHERE ActionId=?",command['ActionId'])
            return command

    @retry_deadlocks
    def release_turn(self,aid,tid,key):
        """Let the operator choose again after the table refused a manual action for this turn."""
        with self.transaction() as db:
            self._manual_lock(db,aid,tid)
            db.execute("DELETE scheduler.TableActionClaim WHERE SiteId=:site AND BotAccountId=? AND TableId=? AND TurnKey=? AND Source='manual'",aid,str(tid),key)

    @retry_deadlocks
    def claim_table_action(self,aid,tid,key,command=None):
        with self.transaction() as db:
            self._manual_lock(db,aid,tid);mode=self._manual_mode(db,aid,tid)
            if command:
                if not mode or str(mode['RequestId'])!=str(command['RequestId']):return False
                pending=db.one("SELECT Status FROM scheduler.ManualTableAction WHERE ActionId=?",command['ActionId'])
                if not pending or pending['Status']!='queued':return False
            elif mode:return False
            # A tournament entry's TableId is the tournament ('T...'); the table it plays is PlayTableId.
            if not db.one('SELECT EntryId FROM scheduler.Entry WHERE SiteId=:site AND MachineId=:machine AND BotAccountId=? AND (TableId=? OR PlayTableId=?) AND IsOpen=1 AND WorkerEpoch=?',aid,str(tid),str(tid),self.epoch):return False
            if db.one('SELECT TurnKey FROM scheduler.TableActionClaim WHERE SiteId=:site AND BotAccountId=? AND TableId=? AND TurnKey=?',aid,str(tid),key):
                if command:db.execute("UPDATE scheduler.ManualTableAction SET Status='expired',ResultText='An action was already submitted for this turn.' WHERE ActionId=?",command['ActionId'])
                return False
            db.execute('INSERT scheduler.TableActionClaim(SiteId,BotAccountId,TableId,TurnKey,Source) VALUES(:site,?,?,?,?)',aid,str(tid),key,'manual' if command else 'automatic')
            if command:db.execute("UPDATE scheduler.ManualTableAction SET Status='submitted',UpdatedUtc=SYSUTCDATETIME() WHERE ActionId=?",command['ActionId'])
            return True

    @retry_deadlocks
    def finish_manual_action(self,action_id,status,detail):
        with self.transaction() as db:
            db.execute('UPDATE scheduler.ManualTableAction SET Status=?,ResultText=?,UpdatedUtc=SYSUTCDATETIME() WHERE ActionId=? AND MachineId=:machine',status,detail[:500],action_id)

    @retry_deadlocks
    def manual_snapshot(self,aid,tid,key):
        with self.transaction() as db:
            mode=self._manual_mode(db,aid,tid)
            claimed=key and db.one('SELECT TurnKey FROM scheduler.TableActionClaim WHERE SiteId=:site AND BotAccountId=? AND TableId=? AND TurnKey=?',aid,str(tid),key)
            return {'supported':True,'enabled':bool(mode),'request_id':str(mode['RequestId']) if mode else None,'submitted':bool(claimed)}

    @retry_deadlocks
    def manual_watches(self):
        with self.transaction() as db:
            return db.rows("""SELECT c.BotAccountId,c.TableId,c.Enabled,
                CASE WHEN EXISTS(SELECT 1 FROM scheduler.ManualTableAction x WHERE x.RequestId=c.RequestId AND x.Status='queued') THEN 1 ELSE 0 END AS Pending
                FROM scheduler.ManualTableControl c JOIN scheduler.LiveViewRequest r ON r.RequestId=c.RequestId
                WHERE c.SiteId=:site AND c.MachineId=:machine AND c.WorkerEpoch=?
                  AND r.Status IN ('requested','active') AND r.ExpiresUtc>SYSUTCDATETIME()""",self.epoch)
