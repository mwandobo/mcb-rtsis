create table MANDATE_SHOW
(
    ACCOUNT_NUMBER CHAR(40) not null,
    PRFT_SYSTEM    SMALLINT not null,
    ID_TRANSACT    INTEGER  not null,
    ID_CHANNEL     INTEGER  not null,
    SCALE_ID       SMALLINT not null,
    INTERNAL_SN    INTEGER  not null,
    DESCRIBE_CHAR  VARCHAR(500),
    DESCRIBE_OR    VARCHAR(5),
    constraint PK_MANDATE_SHOW
        primary key (INTERNAL_SN, SCALE_ID, ID_CHANNEL, ID_TRANSACT, PRFT_SYSTEM, ACCOUNT_NUMBER)
);

