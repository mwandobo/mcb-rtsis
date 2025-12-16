create table ASSET_TRANSACTIONS
(
    ASSET_ID_TRANSACT     INTEGER not null
        constraint IXU_GL_016
            primary key,
    STOCK                 SMALLINT,
    AMOUNT_SUPPLIER       SMALLINT,
    AMOUNT_DEPREC         SMALLINT,
    ASSET_VALUE           SMALLINT,
    ASSET_VALUE_INVENTORY SMALLINT,
    TRX_LUNIT             INTEGER,
    TRX_UNIT              INTEGER,
    TRX_LDATE             DATE,
    TRX_DATE              DATE,
    TRX_LUSR              CHAR(8),
    TRX_USR               CHAR(8),
    STATUS_CODE           VARCHAR(4) default '0',
    REVAL_IND             DECIMAL(1) default 0
);

