-- Slower, varied traffic for Nenad / Evenbet only. Keep other sites unchanged.
UPDATE s SET MinJoinGapSeconds=45,MaxJoinGapSeconds=285,UpdatedUtc=SYSUTCDATETIME()
FROM scheduler.Site s JOIN dbo.Customer c ON c.CustomerId=s.CustomerId
WHERE c.CustomerName=N'Nenad' AND s.Casino=N'Evenbet';
