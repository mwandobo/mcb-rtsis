create table GL_AMORT_RECORDING
(
    ARTICLE_SN      DECIMAL(15) not null,
    TIMESTAMP       TIMESTAMP(6),
    TRX_DATE        DATE        not null,
    TRX_UNIT        INTEGER     not null,
    TRX_USR         CHAR(8)     not null,
    TRX_SN          INTEGER     not null,
    TUN_INTERNAL_SN SMALLINT    not null,
    MOVEMENT_TYPE   CHAR(1),
    AMOUNT          DECIMAL(15, 2),
    constraint IXU_AMORT_REC_001
        primary key (ARTICLE_SN, TRX_DATE, TRX_UNIT, TRX_USR, TRX_SN, TUN_INTERNAL_SN)
);

