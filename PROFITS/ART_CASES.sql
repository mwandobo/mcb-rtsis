create table ART_CASES
(
    CURRENT_DATE          DATE        not null,
    CRN                   VARCHAR(40) not null,
    ENTITY_KEY            VARCHAR(30) not null,
    LEGAL_PROC_START_DATE DATE,
    FINAL_COURT_DECISION  CHAR(1),
    UNDER_ARBITRATION     CHAR(1),
    CASE_AMOUNT           DECIMAL(13, 2),
    CURRENCY_CODE         CHAR(3),
    FILE_ACTION           CHAR(1) default 'F',
    primary key (CURRENT_DATE, CRN, ENTITY_KEY)
);

