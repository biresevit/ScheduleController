IF COL_LENGTH(N'scheduler.LobbyTable',N'TableType') IS NULL
    ALTER TABLE scheduler.LobbyTable ADD TableType varchar(4) NOT NULL
        CONSTRAINT DF_LobbyTable_TableType DEFAULT 'RING';
GO
IF NOT EXISTS(SELECT 1 FROM sys.check_constraints
              WHERE parent_object_id=OBJECT_ID(N'scheduler.LobbyTable')
                AND name=N'CK_LobbyTable_TableType')
    ALTER TABLE scheduler.LobbyTable ADD CONSTRAINT CK_LobbyTable_TableType
        CHECK(TableType IN ('RING','SNG','MTT'));
GO
