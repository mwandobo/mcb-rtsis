create table CREDIT_CARD
(
    ACCOUNT_NO        CHAR(16) not null
        constraint IXU_CIS_169
            primary key,
    INTEREST_ACCRUED  DECIMAL(7, 2),
    FK_CUST_ADD_INFFK INTEGER,
    BALANCE           DECIMAL(9, 2),
    CREDIT_LIMIT      DECIMAL(9, 2),
    EXTRACT_DATE      DATE,
    CURRENCY          CHAR(3),
    ENTRY_TYPE        CHAR(15)
);

