create table REP_74641
(
    CUST_ID          INTEGER,
    UNIT_CODE        INTEGER,
    GENERIC_DET_ID   INTEGER,
    ACCOUNT_BALANCE  DECIMAL(15, 2),
    CURR_CLASSIF     CHAR(2),
    NAME0            CHAR(40),
    GENERIC_DET_DESC CHAR(40)
);

create unique index IXU_REP_013
    on REP_74641 (CUST_ID, UNIT_CODE);

