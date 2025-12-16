create table FIXING_TFI_FEED
(
    TRX_TMSTAMP            TIMESTAMP(6) not null,
    TRX_UNIT               INTEGER      not null,
    TRX_USER               CHAR(8)      not null,
    TRX_DATE               DATE         not null,
    F52_SENDINGTIME        CHAR(40)     not null,
    F55_SYMBOL             CHAR(20)     not null,
    F34_MSGSEQNUM          CHAR(40)     not null,
    F262_MDREQID           CHAR(40)     not null,
    F8_BEGINSTRING         CHAR(40)     not null,
    F9_BODYLENGTH          VARCHAR(20),
    F35_MSGTYPE            VARCHAR(20),
    F49_SENDERCOMPID       VARCHAR(40),
    F56_TARGETCOMPID       VARCHAR(40),
    F268_NOMDENTRIES       VARCHAR(20),
    F269_MDENTRYTYPE_BID   VARCHAR(20),
    F270_MDENTRYPX_BID     DECIMAL(12, 6),
    F269_MDENTRYTYPE_OFFER VARCHAR(20),
    F270_MDENTRYPX_OFFER   DECIMAL(12, 6),
    CALC_FIXING            DECIMAL(12, 6),
    constraint PK_TFI_FEED
        primary key (TRX_UNIT, F262_MDREQID, F34_MSGSEQNUM, F55_SYMBOL, F52_SENDINGTIME, TRX_TMSTAMP, TRX_DATE,
                     TRX_USER)
);

