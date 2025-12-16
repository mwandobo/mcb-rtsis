create table PROFITS_MIGR_AGREEMENTS
(
    NON_MIGR_AGREEMENTS_TOT_LIMIT DECIMAL(15, 2),
    MIGR_AGREEMENTS_TOT_LIMIT     DECIMAL(15, 2),
    AGREEMENTS_TOT_LIMIT          DECIMAL(15, 2),
    NON_MIGR_AGREEMENTS           DECIMAL(10),
    MIGR_AGREEMENTS               DECIMAL(10),
    AGREEMENTS                    DECIMAL(10),
    PRODUCT_ID                    CHAR(5) not null
        constraint PR_MIGR_AGREEMENTS_PK
            primary key,
    SERIAL_NO                     DECIMAL(10)
);

