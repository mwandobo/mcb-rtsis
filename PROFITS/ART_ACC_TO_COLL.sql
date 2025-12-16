create table ART_ACC_TO_COLL
(
    SN                          INTEGER generated always as identity,
    CURRENT_DATE                DATE        not null,
    ACCOUNT_KEY                 VARCHAR(40) not null,
    COLLATERAL_CATEGORY         CHAR(3)     not null,
    TOTAL_RECOV_VAL_PER_CATEGOR DECIMAL(13, 2),
    FILE_ACTION                 CHAR(1) default 'F',
    primary key (SN, CURRENT_DATE, ACCOUNT_KEY, COLLATERAL_CATEGORY)
);

