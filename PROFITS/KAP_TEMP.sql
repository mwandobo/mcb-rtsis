create table KAP_TEMP
(
    ACCOUNT_NUMBER DECIMAL(11) not null,
    CUST_ID        INTEGER     not null,
    UNIT_CODE      INTEGER     not null,
    CUS_C_DIGIT    SMALLINT,
    ACC_C_DIGIT    SMALLINT,
    FIRST_NAME     CHAR(20),
    UNIT_NAME      CHAR(40),
    SURNAME        CHAR(70),
    constraint IXU_DEP_138
        primary key (ACCOUNT_NUMBER, CUST_ID, UNIT_CODE)
);

