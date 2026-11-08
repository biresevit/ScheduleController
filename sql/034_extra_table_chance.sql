-- Per site: the chance (percent) that a planned ring join adds a table to an account that is already playing, instead
-- of logging in another account. Before this the planner always chose an account with the fewest tables, so with
-- many accounts nearly every bot played a single table (Simo, 2026-11-08). Default 30.
IF COL_LENGTH(N'scheduler.Site',N'ExtraTableChancePercent') IS NULL
    ALTER TABLE scheduler.Site ADD ExtraTableChancePercent int NOT NULL
        CONSTRAINT DF_Site_ExtraTableChancePercent DEFAULT 30;
GO
IF OBJECT_ID(N'scheduler.CK_Site_ExtraTableChancePercent') IS NULL
    ALTER TABLE scheduler.Site ADD CONSTRAINT CK_Site_ExtraTableChancePercent
        CHECK (ExtraTableChancePercent BETWEEN 0 AND 100);
