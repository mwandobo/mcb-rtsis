create table AML_DELETE
(
    CUST_CUSTNO      INTEGER  not null,
    VERF_BER         INTEGER  not null,
    PRFT_ACCOUNT_NUM CHAR(40) not null,
    PRFT_SYSTEM      SMALLINT not null,
    BUSINESSTYPE     INTEGER,
    GEN_DET_SER_NUM  INTEGER,
    LAST_UPDATE      DATE,
    ACC_CURRENCYISO  CHAR(3),
    CUST_INSTITUTE   CHAR(4),
    ACCNO            CHAR(11),
    BUSINESSNO       CHAR(11),
    constraint IXU_AML_005
        primary key (CUST_CUSTNO, VERF_BER, PRFT_ACCOUNT_NUM, PRFT_SYSTEM)
);

