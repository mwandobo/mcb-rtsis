create table LNS_CRD_CUST_ADDRESS
(
    REPORTING_DATE DATE        not null,
    CUST_ID        INTEGER     not null,
    ADDRESS_TYPE   VARCHAR(32) not null,
    COUNTRY        CHAR(2),
    DISTRICT       VARCHAR(32),
    HOUSE_NUM      VARCHAR(16),
    ZIP_CODE       VARCHAR(16),
    REGION         VARCHAR(32),
    STREET_WARD    VARCHAR(64),
    CITY           VARCHAR(64),
    constraint CRDCU02
        primary key (REPORTING_DATE, CUST_ID, ADDRESS_TYPE)
);

