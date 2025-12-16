create table REG_REP_04
(
    LA_FINSEC     VARCHAR(15) not null
        constraint REG_REP_04_PK
            primary key,
    BUSINESS_DATE DATE,
    CREATED_APPL  BIGINT,
    REQ_AMT       DECIMAL(19, 2),
    APP_APPL      BIGINT,
    APPR_AMT      DECIMAL(19, 2)
);

