create table MOF_SDOE_PROD_DT
(
    RECORD_TYPE         CHAR(2),
    BATCH_ID            CHAR(12) not null,
    FILE_ID             SMALLINT not null,
    UNIQUE_REF_ID       CHAR(12) not null,
    ACCOUNT_NUMBER      CHAR(27) not null,
    RESPON_BANK_CD      CHAR(3),
    TARGET_BANK_CD      CHAR(3),
    OPEN_UNIT           CHAR(4),
    ACTIVE_ACC_IND      CHAR(1),
    OPEN_DATE           CHAR(10),
    CLOSE_DATE          CHAR(10),
    ACCOUNT_TYPE        CHAR(1),
    CCY_ISO_CODE        CHAR(3),
    LOAN_TYPE           CHAR(2),
    SERVICE_TYPE        CHAR(2),
    LOAN_LIMIT_AMNT     CHAR(18),
    LOAN_END_DATE       CHAR(10),
    LOAN_STATUS         CHAR(2),
    LOAN_COLL_TYPE      CHAR(2),
    START_RQST_BAL      CHAR(18),
    START_TYPE_BAL      CHAR(1),
    END_RQST_BAL        CHAR(18),
    END_TYPE_BAL        CHAR(1),
    ERROR_FIELD         CHAR(2),
    ERROR_CODE          CHAR(2),
    PRFT_ACCOUNT_NUMBER CHAR(40),
    PRFT_ACCOUNT_SYSTEM SMALLINT,
    constraint PK_PROD_DT
        primary key (FILE_ID, ACCOUNT_NUMBER, UNIQUE_REF_ID, BATCH_ID)
);

