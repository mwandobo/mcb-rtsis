create table BOSA_INTER_DATES
(
    PRODUCT_ID        INTEGER not null,
    CR_INTER_ACC_LAST DATE    not null,
    CR_INTER_ACC_NEXT DATE    not null,
    CR_INT_RATE       DECIMAL(12, 6),
    EXECUTION_DATE    DATE,
    PROCESS_FLAG      CHAR(1),
    TIMESTAMP         TIMESTAMP(6),
    MIN_CAP_AMOUNT    DECIMAL(15, 2),
    RETENTION_PERC    DECIMAL(8, 4),
    constraint PK_BOSA_INTER_DATES
        primary key (PRODUCT_ID, CR_INTER_ACC_NEXT, CR_INTER_ACC_LAST)
);

