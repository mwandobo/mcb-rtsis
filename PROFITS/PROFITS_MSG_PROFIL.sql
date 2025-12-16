create table PROFITS_MSG_PROFIL
(
    SN             DECIMAL(12)  not null,
    TMSTAMP        TIMESTAMP(6) not null,
    CUST_ID        INTEGER,
    ACCOUNT_NUMBER CHAR(40),
    PRFT_SYSTEM    SMALLINT,
    PROFILE        CHAR(8)      not null,
    ENTRY_STATUS   CHAR(1),
    constraint PK_MSG_PROFILE
        primary key (PROFILE, TMSTAMP, SN)
);

