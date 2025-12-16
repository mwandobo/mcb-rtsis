create table PROFITS_EMAIL
(
    INSERT_TMSTAMP   TIMESTAMP(6) not null,
    CUST_ID          INTEGER      not null,
    INTERNAL_SN      DECIMAL(10)  not null,
    CONTACT_SN       DECIMAL(10),
    EMAIL_TO         CHAR(254),
    EMAIL_SUBJECT    CHAR(254),
    EMAIL_BODY       VARCHAR(4000),
    EMAIL_ATTACHMENT CHAR(254),
    C_DIGIT          SMALLINT,
    CUST_NAME        CHAR(90),
    INSERT_UNIT      INTEGER,
    INSERT_USER      CHAR(8),
    INSERT_DATE      DATE,
    ACCOUNT_NUMBER   CHAR(40),
    ACCOUNT_CD       SMALLINT,
    PRFT_SYSTEM      SMALLINT,
    constraint PK_EMAIL
        primary key (INTERNAL_SN, CUST_ID, INSERT_TMSTAMP)
);

