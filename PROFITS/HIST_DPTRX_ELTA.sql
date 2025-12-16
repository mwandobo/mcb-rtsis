create table HIST_DPTRX_ELTA
(
    TUN_INTERNAL_SN   SMALLINT    not null,
    TRX_USR_SN        INTEGER     not null,
    TRX_USR           CHAR(8)     not null,
    TRX_DATE          DATE        not null,
    ACCOUNT_NUMBER    DECIMAL(11) not null,
    ACCOUNT_CD        SMALLINT,
    PSB_LINE_SN       INTEGER,
    PREV_TRX_UNIT     INTEGER,
    PSB_PREV_LINE_SN  INTEGER,
    SUPERVISED_UNIT   INTEGER,
    TRX_UNIT          INTEGER,
    TRX_AMOUNT        DECIMAL(15),
    PSB_PREV_LAST_BLN DECIMAL(15),
    PREV_TRX_AMOUNT   DECIMAL(15),
    PSB_LAST_BLN      DECIMAL(15),
    NEW_BALANCE       DECIMAL(15),
    TIMESTMP          DATE,
    PREV_TRX_DATE     DATE,
    REVERSED_FLG      CHAR(1),
    PREV_TYPE         CHAR(1),
    TYPE              CHAR(1),
    ID_NO             CHAR(20),
    BENEF_NAME        CHAR(20),
    BENEF_SURNAME     CHAR(40),
    constraint IXU_DEF_131
        primary key (TUN_INTERNAL_SN, TRX_USR_SN, TRX_USR, TRX_DATE, ACCOUNT_NUMBER)
);

