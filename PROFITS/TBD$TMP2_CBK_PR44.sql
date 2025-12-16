create table TBD$TMP2_CBK_PR44
(
    FK_UNITCODE          INTEGER,
    ACC_TYPE             SMALLINT,
    ACC_SN               INTEGER,
    NUMBER_OF_LOANS      DECIMAL(15, 2),
    ACCOUNT_NAME         VARCHAR(91),
    ACCOUNT_NUMBER       CHAR(40) not null,
    CUST_ID              INTEGER,
    TAX_ID               CHAR(20),
    ACCOUNT_TYPE         VARCHAR(9),
    DIR_OWNER_CUSTID     INTEGER,
    DIRECTOR_TAX_ID      CHAR(20),
    DIRECTOR_OWNER       VARCHAR(91),
    EURO_BOOK_BAL        DECIMAL(15, 2),
    YIELD_LIMIT_AMN      DECIMAL(15, 2),
    CLASS_TYPE           CHAR(1),
    CLASS_VALUE          DECIMAL(15, 2),
    INTEREST_IN_SUSPENSE CHAR(1),
    SPECIFIC_PROVISIONS  CHAR(1),
    CLASS_CATEG          CHAR(1),
    EOM_DATE             DATE     not null
);

