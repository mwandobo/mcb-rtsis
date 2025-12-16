create table PROFITS_MESSAGE
(
    SN                 DECIMAL(12)  not null,
    TMSTAMP            TIMESTAMP(6) not null,
    CUST_ID            INTEGER      not null,
    ACCOUNT_NUMBER     CHAR(40)     not null,
    PRFT_SYSTEM        SMALLINT     not null,
    ACTUAL_MESSAGE     VARCHAR(400),
    MSG_STATUS         CHAR(1),
    ALL_PROFILES       CHAR(1),
    EXPIRATION_DATE    DATE,
    OTHER_CHANNEL      CHAR(1),
    MSG_REASON         CHAR(1),
    CREATE_USER        CHAR(8),
    CREATE_DATE        DATE,
    UPDATE_USER        CHAR(8),
    UPDATE_DATE        DATE,
    AUTOMATIC_CREATION CHAR(1),
    AUTOMATIC_REASON   VARCHAR(40),
    FK_GH_MSG_TYPE     CHAR(5),
    FK_GD_MSG_TYPE     INTEGER,
    MANDATORY_DETAILS  CHAR(1),
    CREATE_UNIT        INTEGER,
    UPDATE_UNIT        INTEGER,
    constraint PK_PROFITS_MSG
        primary key (PRFT_SYSTEM, ACCOUNT_NUMBER, CUST_ID, TMSTAMP, SN)
);

create unique index IX_PROFITS_MESSAGE_777
    on PROFITS_MESSAGE (CUST_ID, MSG_STATUS, SN, TMSTAMP);

