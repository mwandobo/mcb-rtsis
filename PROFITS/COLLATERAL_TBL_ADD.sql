create table COLLATERAL_TBL_ADD
(
    RECORD_TYPE        CHAR(2) not null
        constraint PK_CTBLDTADD
            primary key,
    RECORD_DESC        VARCHAR(40),
    HAS_OWNERS         CHAR(1),
    HAS_DOCUMENTATION  CHAR(1),
    HAS_INSURANCE      CHAR(1),
    HAS_VALUATION      CHAR(1),
    HAS_DEALERS        CHAR(1),
    HAS_TRACKING       CHAR(1),
    HAS_REPOSSESION    CHAR(1),
    HAS_DEBENT_CHATTEL CHAR(1)
);

