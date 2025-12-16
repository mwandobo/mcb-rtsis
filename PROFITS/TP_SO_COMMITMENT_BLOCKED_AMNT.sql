create table TP_SO_COMMITMENT_BLOCKED_AMNT
(
    TP_SO_IDENTIFIER DECIMAL(10)  not null,
    TRX_UNIT         INTEGER      not null,
    TRX_DATE         DATE         not null,
    TRX_USR          CHAR(8)      not null,
    TRX_USR_SN       INTEGER      not null,
    BLOCK_STATUS     CHAR(1)      not null,
    TIMESTMP         TIMESTAMP(6) not null
);

create unique index PK_TP_SO_COMMITMENT_BLOCK_AMN
    on TP_SO_COMMITMENT_BLOCKED_AMNT (TP_SO_IDENTIFIER, TRX_DATE, TRX_UNIT, TRX_USR, TRX_USR_SN);

