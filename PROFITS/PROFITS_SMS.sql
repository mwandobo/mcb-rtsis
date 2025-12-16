create table PROFITS_SMS
(
    INSERT_TMSTAMP TIMESTAMP(6) not null,
    CUST_ID        INTEGER      not null,
    INTERNAL_SN    DECIMAL(10)  not null,
    CONTACT_SN     DECIMAL(10),
    SMS_TO         CHAR(254),
    SMS_SUBJECT    CHAR(254),
    SMS_BODY       VARCHAR(4000),
    C_DIGIT        SMALLINT,
    CUST_NAME      CHAR(90),
    INSERT_UNIT    INTEGER,
    INSERT_USER    CHAR(8),
    INSERT_DATE    DATE,
    ACCOUNT_NUMBER CHAR(40),
    ACCOUNT_CD     SMALLINT,
    PRFT_SYSTEM    SMALLINT,
    PROCESSED_FLG  CHAR(1),
    constraint PK_SMS
        primary key (INTERNAL_SN, CUST_ID, INSERT_TMSTAMP)
);

