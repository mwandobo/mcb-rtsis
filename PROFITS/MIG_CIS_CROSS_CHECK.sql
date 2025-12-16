create table MIG_CIS_CROSS_CHECK
(
    FAILED_COUNT DECIMAL(10),
    MG_COUNT     DECIMAL(10) not null
        constraint MIG_CIS_CROSS_CHECK_PKEY
            primary key,
    PASS_COUNT   DECIMAL(10)
);

