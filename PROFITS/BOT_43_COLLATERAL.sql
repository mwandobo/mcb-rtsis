create table BOT_43_COLLATERAL
(
    COLLATERAL_ID        INTEGER generated always as identity
        constraint BOT_43_COLLATERAL_ID_PK
            primary key,
    COLLATERALTYPE       INTEGER not null,
    COLLATERALVALUE      DECIMAL(19, 4),
    COMMENT1             VARCHAR(128),
    REGISTEREDCOLLATERAL INTEGER,
    LOAN_CODE            VARCHAR(40),
    REPORTING_DATE       DATE,
    CUST_ID              VARCHAR(32),
    COLLATERAL_SN        DECIMAL(10),
    COLL_TYPE            INTEGER,
    COLL_UNIT            INTEGER
);

