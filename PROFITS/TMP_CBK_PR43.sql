create table TMP_CBK_PR43
(
    BOOK_BALANCE         DECIMAL(15, 2),
    INTEREST_IN_SUSPENSE DECIMAL(15, 2),
    PROVISION_AMOUNT     DECIMAL(15, 2),
    ACCOUNT_NUMBER       CHAR(40) not null,
    ACCOUNT_CD           SMALLINT,
    FIRST_NAME           CHAR(20),
    SURNAME              CHAR(70),
    AFM_NO               CHAR(20),
    CUST_ID              INTEGER,
    CURR_SUB_CLASS       CHAR(1),
    FIRST_NAME_REP       CHAR(20),
    SURNAME_REP          CHAR(70),
    AFM_REP              CHAR(20),
    CUST_REP             INTEGER,
    EOM_DATE             DATE     not null
);

