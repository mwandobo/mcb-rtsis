create table MANDATE_COND_HD
(
    ACCOUNT_NUMBER CHAR(40) not null,
    PRFT_SYSTEM    SMALLINT not null,
    ID_TRANSACT    INTEGER  not null,
    ID_CHANNEL     INTEGER  not null,
    SCALE_ID       SMALLINT not null,
    CONDITION_SN   INTEGER  not null,
    DESCRIPTION    VARCHAR(250),
    TIMESTAMP      TIMESTAMP(6),
    constraint PK_MANDATE_COND_HD
        primary key (CONDITION_SN, SCALE_ID, ID_CHANNEL, ID_TRANSACT, PRFT_SYSTEM, ACCOUNT_NUMBER)
);

