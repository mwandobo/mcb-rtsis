create table EOM_TOP_LOANS
(
    FK_UNITCODE           INTEGER,
    ACC_TYPE              SMALLINT,
    ACC_SN                INTEGER,
    NUMBER_OF_LOANS       INTEGER,
    ACCOUNT_NAME          VARCHAR(100),
    ACCOUNT_NUMBER        CHAR(40) not null,
    CUST_ID               INTEGER,
    TAX_ID                VARCHAR(20),
    ACCOUNT_TYPE          VARCHAR(9),
    DIR_OWNER_CUSTID      INTEGER,
    DIRECTOR_TAX_ID       VARCHAR(20),
    DIRECTOR_OWNER        VARCHAR(100),
    SECTOR                VARCHAR(80),
    CREDIT_FACILITIES     VARCHAR(80),
    DRAWDOWN_FST_DT       DATE,
    ACC_LIMIT_AMN         DECIMAL(15, 2),
    EURO_BOOK_BAL         DECIMAL(15, 2),
    YIELD_LIMIT_AMN       DECIMAL(15, 2),
    GROUP_TOTAL           DECIMAL(15, 2),
    CLASS_CATEG           VARCHAR(80),
    CLASS_TYPE            VARCHAR(80),
    CLASS_VALUE           DECIMAL(15, 2),
    AMOUNT_CHARGED        DECIMAL(15, 2),
    VALUATION_DT          DATE,
    VALUER                VARCHAR(100),
    INTEREST_IN_SUSPENSE  VARCHAR(80),
    SPECIFIC_PROVISIONS   VARCHAR(80),
    ADDITIONAL_PROVISIONS VARCHAR(80),
    EOM_DATE              DATE     not null,
    AGR_LIMIT             DECIMAL(15, 2),
    GROSS_TOTAL           DECIMAL(15, 2)
);

create unique index EOM_TOP_LOANS_PK
    on EOM_TOP_LOANS (EOM_DATE, FK_UNITCODE, ACC_TYPE, ACC_SN);

create unique index IDX_EOM_TOP_LOANS
    on EOM_TOP_LOANS (EOM_DATE, CUST_ID);

