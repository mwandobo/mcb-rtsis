create table CORRESP_EXTR_SWT
(
    CUSTOMER_NUMBER INTEGER     not null,
    ACCOUNT_NUMBER  DECIMAL(11) not null,
    TRX_DATE        DATE        not null,
    TRX_UNIT        INTEGER     not null,
    TRX_USR         CHAR(8)     not null,
    TRX_SN          INTEGER     not null,
    PRFT_REF_NO     CHAR(16),
    TRX_REF_NO_20   CHAR(16),
    MSG_CATEGORY    CHAR(1),
    SENDER_BIC      CHAR(11),
    RECEIVER_BIC    CHAR(11),
    VALUE_DATE      DATE,
    FIN_COPY        VARCHAR(9),
    STATUS          CHAR(1),
    TMSTAMP         TIMESTAMP(6),
    EXTR_COMMENTS10 VARCHAR(40),
    constraint PK_CORRESP_EXTR_SWT
        primary key (CUSTOMER_NUMBER, ACCOUNT_NUMBER, TRX_SN, TRX_USR, TRX_UNIT, TRX_DATE)
);

create unique index SK_CORRESP_EXTR_SWT
    on CORRESP_EXTR_SWT (TRX_REF_NO_20);

