create table MOF_SDOE_RSLTS_DT
(
    RECORD_TYPE     CHAR(2),
    BATCH_ID        CHAR(12) not null,
    FILE_ID         SMALLINT not null,
    UNIQUE_REF_ID   CHAR(12) not null,
    CUSTOMER_CD     CHAR(40) not null,
    MOF_TAXID       CHAR(9),
    SN              CHAR(3),
    RESULT_CD       CHAR(2),
    RESLTS_TIMESTMP CHAR(26),
    RESPON_BANK_CD  CHAR(3),
    TARGET_BANK_CD  CHAR(3),
    ERROR_FIELD     CHAR(2),
    ERROR_CODE      CHAR(2),
    constraint PK_RESULTS_DT
        primary key (CUSTOMER_CD, UNIQUE_REF_ID, FILE_ID, BATCH_ID)
);

