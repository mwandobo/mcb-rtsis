create table CUST_ACC_CHARGES
(
    ACCOUNT_NUMBER   CHAR(40) not null,
    PRFT_SYSTEM      SMALLINT not null,
    CUST_ID          INTEGER  not null,
    CHARGE_TYPE      CHAR(1)  not null,
    CHARGE_CODE      INTEGER  not null,
    REQUEST_SN       SMALLINT not null,
    REQUEST_TYPE     CHAR(1)  not null,
    REQUEST_LOAN_STS CHAR(1)  not null,
    CHARGES_CURR_ID  INTEGER  not null,
    CR_AMOUNT        DECIMAL(15, 2),
    DB_AMOUNT        DECIMAL(15, 2),
    constraint CUST_ACC_CH_PK
        primary key (CHARGES_CURR_ID, REQUEST_LOAN_STS, REQUEST_TYPE, REQUEST_SN, CHARGE_CODE, CHARGE_TYPE, CUST_ID,
                     PRFT_SYSTEM, ACCOUNT_NUMBER)
);

create unique index IX_CUST_CHARG
    on CUST_ACC_CHARGES (ACCOUNT_NUMBER, CHARGE_TYPE, CHARGE_CODE);

