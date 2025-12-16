create table W_FACT_INSURANCE_APPLICATION
(
    EFF_FROM_DATE      DATE        not null,
    EFF_TO_DATE        DATE        not null,
    ROW_CURRENT_FLAG   SMALLINT,
    TP_SO_ID           DECIMAL(10) not null,
    ACCT_KEY           DECIMAL(11) not null,
    ANNUAL_PREMIUM     DECIMAL(15, 2),
    PRORATA_PREMIUM    DECIMAL(15, 4),
    MONTHLY_PREMIUM    DECIMAL(15, 2),
    REMAINING_AMNT     DECIMAL(15, 2),
    ISS_COMMITMENT_KEY DECIMAL(10),
    ADD_INSTALM_AMNT   DECIMAL(15, 2),
    constraint PK_W_FACT_ISS_ACCT_ALLOC
        primary key (EFF_FROM_DATE, TP_SO_ID)
);

