create table SUP_TRANSACTIONS_D
(
    FK_SUP_ID_TRANSACT  DECIMAL(5) not null,
    REL_SUP_ID_TRANSACT DECIMAL(5) not null,
    TRX_LUNIT           DECIMAL(5),
    TRX_UNIT            DECIMAL(5),
    TRX_LDATE           DATE,
    TRX_DATE            DATE,
    TRX_LUSR            CHAR(8),
    TRX_USR             CHAR(8),
    STATUS_CODE         CHAR(4) default '0',
    COMMENTS            CHAR(200),
    constraint IXU_SUP_0002
        primary key (FK_SUP_ID_TRANSACT, REL_SUP_ID_TRANSACT)
);

