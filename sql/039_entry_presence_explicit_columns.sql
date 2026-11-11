-- SELECT e.* cached the old Entry column layout. Adding IsParked shifted the
-- reporting flags: IsLiveOwner exposed IsParked and IsConfirmedSeat exposed
-- IsLiveOwner. Explicit columns both refresh metadata and prevent future drift.
CREATE OR ALTER VIEW backoffice.EntryPresence AS
SELECT e.EntryId,e.SiteId,e.BotAccountId,e.MachineId,e.WorkerEpoch,e.TableId,
 e.Status,e.IsOpen,e.BuyInAmount,e.CreatedUtc,e.SeatedUtc,e.ClosedUtc,
 e.LastConfirmedUtc,e.SessionId,e.PlayTableId,e.IsParked,
 CONVERT(bit,CASE WHEN owner.IsReady=1 AND owner.WorkerEpoch=e.WorkerEpoch
   AND owner.LastHeartbeatUtc>=DATEADD(second,-180,SYSUTCDATETIME()) THEN 1 ELSE 0 END) AS IsLiveOwner,
 CONVERT(bit,CASE WHEN e.IsOpen=1 AND owner.IsReady=1 AND owner.WorkerEpoch=e.WorkerEpoch
   AND owner.LastHeartbeatUtc>=DATEADD(second,-180,SYSUTCDATETIME()) AND EXISTS(
  SELECT 1 FROM scheduler.LobbyTable t
  JOIN scheduler.MachineAssignment observer ON observer.SiteId=t.SiteId AND observer.MachineId=t.ObserverMachineId
  JOIN scheduler.TableSeat seat ON seat.SiteId=t.SiteId AND seat.TableId=t.TableId
  JOIN dbo.BotAccount bot ON bot.BotAccountId=e.BotAccountId
  WHERE t.SiteId=e.SiteId AND t.TableId=e.TableId
   AND t.FullSnapshotUtc>=DATEADD(second,-180,SYSUTCDATETIME())
   AND t.ObserverEpoch=observer.WorkerEpoch AND observer.IsReady=1
   AND observer.LastHeartbeatUtc>=DATEADD(second,-180,SYSUTCDATETIME())
   AND seat.ObservedUtc>=DATEADD(second,-180,SYSUTCDATETIME())
   AND (seat.BotAccountId=e.BotAccountId OR seat.Username=bot.Username)
 ) THEN 1 ELSE 0 END) AS IsConfirmedSeat
FROM scheduler.Entry e
LEFT JOIN scheduler.MachineAssignment owner ON owner.SiteId=e.SiteId AND owner.MachineId=e.MachineId;
