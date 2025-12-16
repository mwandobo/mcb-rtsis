create table LNS_CRD_COLL_SUB_DATA
(
    REPORTING_DATE   DATE        not null,
    LOAN_CODE        VARCHAR(40) not null,
    COLL_TYPE        INTEGER     not null,
    COLLATERAL_SN    DECIMAL(10) not null,
    COLL_UNIT        INTEGER     not null,
    INTERNAL_SN      SMALLINT    not null,
    COLLATERAL_VALUE DECIMAL(15, 2),
    COLLATERAL_TYPE  VARCHAR(128),
    CUST_ID          INTEGER,
    constraint CRDLO03
        primary key (REPORTING_DATE, LOAN_CODE, COLL_TYPE, COLLATERAL_SN, COLL_UNIT, INTERNAL_SN)
);

