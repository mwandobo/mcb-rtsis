create table LOAN_SCENARIO_CINS
(
    INSTALL_SN        DECIMAL(10)  not null,
    TMSTAMP           TIMESTAMP(6) not null,
    RECORD_SN         SMALLINT     not null,
    MAIN_COINSURED    CHAR(1),
    COINSURED_CUST_DT DATE,
    INSURANCE_PROD_ID INTEGER,
    constraint PK_SCN_COINS
        primary key (RECORD_SN, TMSTAMP, INSTALL_SN)
);

