create table BLOCK_TRANSACTION
(
    TRX_DATE           DATE,
    TRX_UNIT           INTEGER,
    TRX_USR            CHAR(8),
    TRX_USR_SN         INTEGER,
    O_TRX_UNIT         INTEGER,
    FK_JUSTIFICID_JUST INTEGER,
    O_TRX_USR_SN       INTEGER,
    BLOCK_CNT          DECIMAL(10),
    PLEDGE_SN          DECIMAL(10),
    FK_DEPOSIT_ACCOACC DECIMAL(11),
    AMOUNT             DECIMAL(15, 2),
    EXPIRY_DATE        DATE,
    TIMESTMP           TIMESTAMP(6),
    O_TRX_DATE         DATE,
    POSEXCEPTION_FLG   CHAR(1),
    ENTRY_STATUS       CHAR(1),
    O_TRX_USR          CHAR(8),
    ENTRY_COMMENTS     VARCHAR(40),
    FROM_OTHER_SYS     CHAR(1),
    FIRST_NAME         VARCHAR(20),
    LAST_NAME          VARCHAR(20)
);

create unique index IXU_BLO_000
    on BLOCK_TRANSACTION (TRX_DATE, TRX_UNIT, TRX_USR, TRX_USR_SN);

