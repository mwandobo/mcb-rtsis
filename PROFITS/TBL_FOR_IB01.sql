create table TBL_FOR_IB01
(
    ACCOUNT                    CHAR(40) not null
        constraint IXU_REP_067
            primary key,
    C_DIGIT                    SMALLINT,
    ACCOUNT_CD                 SMALLINT,
    TRANSACTION_CODE           INTEGER,
    ACC_UNIT                   INTEGER,
    CUST_ID                    INTEGER,
    PERCENTAGE                 DECIMAL(15, 4),
    PROPERTY_ACCOUNT_VAL_EUR   DECIMAL(15, 2),
    PROPERTY_ACCOUNT_EUR       DECIMAL(15, 2),
    PROPERTY_AGREEMENT_VAL_EUR DECIMAL(15, 2),
    PROPERTY_AGREEMENT_EUR     DECIMAL(15, 2),
    EUR_BALANCE                DECIMAL(15, 2),
    TRX_DATE                   DATE,
    ACC_OPEN_DT                DATE
);

