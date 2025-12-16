create table MG_AUTO_PRD_DISTR
(
    VALIDITY_DATE DATE    not null,
    PRODUCT_ID    INTEGER not null,
    PROCESS_DATE  DATE,
    ENTRY_STATUS  CHAR(1),
    PRFT_SYSTEM   CHAR(2),
    ROW_ERR_DESC  VARCHAR(100),
    constraint IXU_MIG_012
        primary key (VALIDITY_DATE, PRODUCT_ID)
);

