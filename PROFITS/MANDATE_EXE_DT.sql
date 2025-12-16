create table MANDATE_EXE_DT
(
    PARTICIPANT_SN     SMALLINT not null,
    CUST_ID            INTEGER,
    STATUS             CHAR(1),
    REJECTION_COMMENTS VARCHAR(150),
    ID_CHANNEL         INTEGER  not null,
    TRX_REFER_NO       CHAR(40) not null,
    CONDITION_SN       INTEGER  not null,
    SCALE_ID           SMALLINT not null,
    ID_TRANSACT        INTEGER  not null,
    PRFT_SYSTEM        SMALLINT not null,
    ACCOUNT_NUMBER     CHAR(40) not null,
    TIMESTAMP          TIMESTAMP(6),
    PARAMETER_TYPE     CHAR(5),
    SERIAL_NUM         INTEGER,
    ORDER_SN           INTEGER,
    constraint PK_MANDATE_EXE_DT
        primary key (ID_CHANNEL, TRX_REFER_NO, CONDITION_SN, SCALE_ID, ID_TRANSACT, PRFT_SYSTEM, ACCOUNT_NUMBER,
                     PARTICIPANT_SN)
);

