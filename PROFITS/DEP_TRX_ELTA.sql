create table DEP_TRX_ELTA
(
    ACCOUNT_NUMBER    DECIMAL(11) not null,
    TRX_UNIT          INTEGER     not null,
    TRX_DATE          DATE        not null,
    TRX_USR           CHAR(8)     not null,
    TRX_USR_SN        INTEGER     not null,
    TUN_INTERNAL_SN   SMALLINT    not null,
    ACCOUNT_CD        SMALLINT,
    PSB_LINE_SN       INTEGER,
    SUPERVISED_UNIT   INTEGER,
    PSB_PREV_LINE_SN  INTEGER,
    PREV_TRX_UNIT     INTEGER,
    PSB_LAST_BLN      DECIMAL(15, 2),
    TRX_AMOUNT        DECIMAL(15, 2),
    PREV_TRX_AMOUNT   DECIMAL(15, 2),
    NEW_BALANCE       DECIMAL(15, 2),
    PSB_PREV_LAST_BLN DECIMAL(15, 2),
    TIMESTMP          DATE,
    VALUE_DATE        DATE,
    PREV_TRX_DATE     DATE,
    PREV_TYPE         CHAR(1),
    TYPE              CHAR(1),
    TRANFERRED_FLG    CHAR(1),
    REVERSED_FLG      CHAR(1),
    BENEF_NAME        CHAR(20),
    ID_NO             CHAR(20),
    BENEF_SURNAME     CHAR(40),
    constraint PKDETREL
        primary key (ACCOUNT_NUMBER, TRX_UNIT, TRX_DATE, TRX_USR, TRX_USR_SN, TUN_INTERNAL_SN)
);

