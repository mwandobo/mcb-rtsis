create table TEMP_REP_CD_CHNGS
(
    PROFITS_ACCOUNT CHAR(40),
    DEP_ACC_NUMBER  DECIMAL(11) not null
        constraint PKTMPRCD
            primary key,
    OLD_CDIGIT      SMALLINT,
    NEW_C_DIGIT     SMALLINT,
    OLD_IBAN        CHAR(37),
    NEW_IBAN        CHAR(37),
    CUST_ID         INTEGER,
    FIRST_NAME      CHAR(20),
    SURNAME         CHAR(70),
    BOOK_BALANCE    DECIMAL(15, 2)
);

