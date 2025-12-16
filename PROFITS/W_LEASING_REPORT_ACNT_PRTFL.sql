create table W_LEASING_REPORT_ACNT_PRTFL
(
    EOM_DATE              DATE         not null,
    PRODUCT               INTEGER,
    UNIT                  INTEGER,
    CURRENCY_ID           INTEGER,
    LOAN_STATUS           CHAR(1),
    ACC_STATUS            CHAR(1),
    ACCOUNT_NUMBER        VARCHAR(100) not null,
    START_DATE            DATE,
    END_DATE              DATE,
    CONTRACT_STATUS       VARCHAR(2),
    CURRENCY              VARCHAR(100),
    EQUIPMENT_TYPE        VARCHAR(100),
    LEASE_PURPOSE         VARCHAR(100),
    FUTURE_RENTALS        DECIMAL(15, 2),
    DIFFERED_INTEREST     DECIMAL(15, 2),
    ARREARS               INTEGER,
    LEASE_PROCESSING_FEES DECIMAL(15, 2),
    INTEREST_RATE         DECIMAL(9, 6),
    RPOC_FEES_PERC_CHARG  VARCHAR(10),
    RELATIONSHIP_OFFICER  VARCHAR(100),
    DAYS_IN_ARREARS       INTEGER,
    SECTOR                VARCHAR(100),
    SPREAD                DECIMAL(9, 6),
    primary key (EOM_DATE, ACCOUNT_NUMBER)
);

