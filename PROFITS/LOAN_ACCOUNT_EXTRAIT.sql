create table LOAN_ACCOUNT_EXTRAIT
(
    ACC_UNIT           INTEGER      not null,
    ACC_TYPE           SMALLINT     not null,
    ACC_SN             INTEGER      not null,
    TMSTAMP            TIMESTAMP(6) not null,
    TRX_INTERNAL_SN    SMALLINT     not null,
    ACC_CD             SMALLINT,
    TRX_UNIT           INTEGER,
    TRX_USR            CHAR(8),
    TRX_DATE           DATE,
    TRX_SN             INTEGER,
    ENTRY_STATUS       CHAR(1),
    TRANSACTION_CODE   INTEGER,
    JUSTIFICATION_CODE INTEGER,
    REVERSED_FLG       CHAR(1),
    constraint PIX_LNACCEXT
        primary key (ACC_TYPE, ACC_SN, TMSTAMP, TRX_INTERNAL_SN, ACC_UNIT)
);

