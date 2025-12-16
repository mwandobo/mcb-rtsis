create table OVERDUE_REMINDER_LETTERS
(
    INTERNAL_ROW_SN SMALLINT not null,
    UNIT            INTEGER  not null,
    ACC_SN          INTEGER  not null,
    ACC_TYPE        SMALLINT not null,
    CUSTOMER_TYPE   SMALLINT,
    REMINDER_TYPE   SMALLINT,
    PRFT_ACC_CD     SMALLINT,
    CUST_ID         INTEGER,
    OV_AMOUNT       DECIMAL(15, 2),
    OV_EXP_DT       DATE,
    ISSUE_DATE      DATE,
    TIMESTAMP0      TIMESTAMP(6),
    ZIPCODE         CHAR(10),
    FIRST_NAME      CHAR(20),
    FATHER_NAME     CHAR(20),
    CITY            CHAR(30),
    UNIT_NAME       CHAR(40),
    ADDRESS2        CHAR(40),
    PRFT_ACC_NUMBER CHAR(40),
    ADDRESS1        CHAR(40),
    SURNAME         CHAR(70),
    constraint IXU_LNS_037
        primary key (INTERNAL_ROW_SN, UNIT, ACC_SN, ACC_TYPE)
);

