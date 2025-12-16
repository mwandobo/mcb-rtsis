create table SAFE_DEPOSIT_BOX
(
    SDB_SN             INTEGER not null,
    LAST_TRX_DT        DATE,
    LAST_TRX_USR       CHAR(8),
    SDB_COMMENTS       VARCHAR(240),
    RENTAL_CNT         INTEGER,
    TMSTAMP            TIMESTAMP(6),
    SDB_STATUS         CHAR(1) not null,
    FK_UNITCODE        INTEGER not null,
    FK_SDB_TYPEFK_PROD INTEGER,
    BLOCK_REASON       VARCHAR(240),
    constraint IXU_DEP_106
        primary key (FK_UNITCODE, SDB_SN)
);

