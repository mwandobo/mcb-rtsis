create table AUDIT_LOG_HD
(
    TRX_DATE        DATE    not null,
    TRX_UNIT        INTEGER not null,
    TRX_USER        CHAR(8) not null,
    TRX_USR_SN      INTEGER not null,
    INTERNAL_SN     INTEGER not null,
    TERMINAL_NUMBER CHAR(30),
    TRX_CODE        INTEGER,
    AUTHORIZER1     CHAR(8),
    AUTHORIZER2     CHAR(8),
    PRFT_SYSTEM     INTEGER,
    ACCOUNT_NUMBER  CHAR(40),
    CUST_ID         INTEGER,
    TMPSTAMP        TIMESTAMP(6)
);

create unique index PK_AUDIT_HD
    on AUDIT_LOG_HD (TRX_DATE, TRX_UNIT, TRX_USER, TRX_USR_SN, INTERNAL_SN);

