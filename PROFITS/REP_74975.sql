create table REP_74975
(
    ACCOUNT_CD        SMALLINT,
    INSTALL_FREQ      SMALLINT,
    CURR_INTEREST_PRD SMALLINT,
    CURRENCY_ID       INTEGER,
    UNIT_CODE         INTEGER,
    ID_PRODUCT        INTEGER,
    RATE              DECIMAL(8, 4),
    PERCENTAGE        DECIMAL(9, 6),
    EURO_BOOK_BAL     DECIMAL(15, 2),
    OV_BAL            DECIMAL(15, 2),
    BOOK_BAL          DECIMAL(15, 2),
    NRM_BAL           DECIMAL(15, 2),
    ACC_STATUS        CHAR(1),
    LOAN_STATUS       CHAR(1),
    ACC_MECHANISM     CHAR(1),
    ACCOUNT_NUMBER    CHAR(40),
    UNIT_NAME         VARCHAR(40),
    PROD_DESC         VARCHAR(40)
);

