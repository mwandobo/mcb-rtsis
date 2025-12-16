create table DHSSE_INTERFACED
(
    CURRENCY_ID        INTEGER        not null,
    CHEQUE_AMOUNT      DECIMAL(15, 2) not null,
    ACH_UNIT_CODE      SMALLINT       not null,
    CHEQUE_ACC_TYPE    CHAR(1)        not null,
    CHEQUE_ACC_NUMBER  SMALLINT       not null,
    CHEQUE_ACC_N_SHORT SMALLINT,
    CHEQUE_NUMBER      SMALLINT       not null,
    TMSTAMP            TIMESTAMP(6),
    FK_REFERS_CLAGENT  INTEGER        not null,
    constraint PK_ACH_INTERFACE
        primary key (FK_REFERS_CLAGENT, CURRENCY_ID)
);

