-- Per site: a join pace that grows with the target. A fixed MinJoinGapSeconds..MaxJoinGapSeconds gap between any two
-- joins caps a site at ~90 joins an hour, fewer than the sessions that end each hour once the target is large (Simo
-- plateaued near 75 of 100, 2026-11-09). The scheduler now joins enough to replace ending sessions plus the shortfall
-- spread over FillMinutes (0 = the fixed pace), never faster than SiteJoinsPerMinute for the site; MinJoinGapSeconds
-- becomes the minimum between two logins on one PC.
IF COL_LENGTH(N'scheduler.Site',N'FillMinutes') IS NULL
    ALTER TABLE scheduler.Site ADD FillMinutes int NOT NULL
        CONSTRAINT DF_Site_FillMinutes DEFAULT 45;
GO
IF COL_LENGTH(N'scheduler.Site',N'SiteJoinsPerMinute') IS NULL
    ALTER TABLE scheduler.Site ADD SiteJoinsPerMinute int NOT NULL
        CONSTRAINT DF_Site_SiteJoinsPerMinute DEFAULT 60;
GO
IF OBJECT_ID(N'scheduler.CK_Site_FillMinutes') IS NULL
    ALTER TABLE scheduler.Site ADD CONSTRAINT CK_Site_FillMinutes CHECK (FillMinutes BETWEEN 0 AND 1440);
GO
IF OBJECT_ID(N'scheduler.CK_Site_SiteJoinsPerMinute') IS NULL
    ALTER TABLE scheduler.Site ADD CONSTRAINT CK_Site_SiteJoinsPerMinute CHECK (SiteJoinsPerMinute BETWEEN 1 AND 600);
