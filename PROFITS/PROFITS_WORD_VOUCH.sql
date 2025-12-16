create table PROFITS_WORD_VOUCH
(
    TMSTAMP            TIMESTAMP(6) not null,
    TRX_DATE           DATE         not null,
    TRX_UNIT           INTEGER      not null,
    TRX_USER           CHAR(8)      not null,
    TRX_USR_SN         INTEGER      not null,
    INTERNAL_SN        SMALLINT     not null,
    VOUCH_INT_SN       SMALLINT     not null,
    CURRENCY_ID        INTEGER,
    RESULT_RATE        DECIMAL(12, 6),
    RESULT_AMOUNT      DECIMAL(15, 2),
    RESULT_DATE        DATE,
    CURR_SHORT_DESCR   VARCHAR(3),
    AMOUNT_DESCRIPTION VARCHAR(15),
    TARGET_AMOUNT      VARCHAR(18),
    RESULT_TEXT        VARCHAR(80),
    constraint IXU_PRD_023
        primary key (TMSTAMP, TRX_DATE, TRX_UNIT, TRX_USER, TRX_USR_SN, INTERNAL_SN, VOUCH_INT_SN)
);

