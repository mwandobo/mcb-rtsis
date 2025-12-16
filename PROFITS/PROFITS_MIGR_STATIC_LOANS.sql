create table PROFITS_MIGR_STATIC_LOANS
(
    NON_MIGR_LOANS_TOT_LIMIT DECIMAL(15, 2) not null,
    MIGR_LOANS_TOT_LIMIT     DECIMAL(15, 2),
    LOANS_TOT_LIMIT          DECIMAL(15, 2),
    NON_MIGR_LOANS           DECIMAL(10),
    MIGR_LOANS               DECIMAL(10),
    LOANS                    DECIMAL(10),
    PRODUCT_ID               CHAR(5)        not null
        constraint PR_MIGR_STATIC_LOANS_PK
            primary key
);

