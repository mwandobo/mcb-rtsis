create table MANDATE_DTL
(
    SCALE_ID       SMALLINT not null,
    AMOUNT_FROM    DECIMAL(15, 2),
    AMOUNT_TO      DECIMAL(15, 2),
    ID_CHANNEL     INTEGER  not null,
    ID_TRANSACT    INTEGER  not null,
    PRFT_SYSTEM    SMALLINT not null,
    ACCOUNT_NUMBER CHAR(40) not null,
    HAS_MANDATE    CHAR(1),
    ALL_TO_SIGN    CHAR(1),
    ANY_TO_SIGN    CHAR(1),
    DESCRIPTION    CHAR(250),
    TIMESTAMP      TIMESTAMP(6),
    constraint PK_MANDATE_DTL
        primary key (ID_CHANNEL, ID_TRANSACT, PRFT_SYSTEM, ACCOUNT_NUMBER, SCALE_ID)
);

