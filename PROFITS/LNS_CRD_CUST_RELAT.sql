create table LNS_CRD_CUST_RELAT
(
    REPORTING_DATE     DATE        not null,
    PRIMARY_CUST       INTEGER     not null,
    PRIMARY_CUST_TP    CHAR(1),
    SECONDARY_CUST     INTEGER     not null,
    SECONDARY_CUST_TP  CHAR(1),
    CUST_RELATION_TYPE VARCHAR(32) not null,
    constraint CRDCU03
        primary key (REPORTING_DATE, PRIMARY_CUST, SECONDARY_CUST, CUST_RELATION_TYPE)
);

