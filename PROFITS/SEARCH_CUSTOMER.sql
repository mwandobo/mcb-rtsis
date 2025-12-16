create table SEARCH_CUSTOMER
(
    TRX_USR         CHAR(8)      not null,
    TMSTAMP         TIMESTAMP(6) not null,
    ENTRY_SN        INTEGER      not null,
    CUST_ID         INTEGER      not null,
    CUST_CD         SMALLINT,
    CUST_TYPE       CHAR(1),
    ACCOUNT_NUMBER  CHAR(40),
    ACCOUNT_CD      SMALLINT,
    PRFT_SYSTEM     SMALLINT,
    AFM_NO          CHAR(20),
    ADDR_CNTRY_DESC CHAR(40),
    CITY            CHAR(30),
    ZIP_CODE        CHAR(10),
    ADDRESS_1       CHAR(40),
    ADDRESS_2       CHAR(40),
    TELEPHONE       CHAR(15),
    ID_TYPE_DESCR   CHAR(40),
    ID_TYPE_SN      INTEGER,
    ID_NO           CHAR(20),
    ISSUE_AUTHORITY CHAR(30),
    FATHER_NAME     CHAR(20),
    MOTHER_NAME     CHAR(20),
    NON_RESIDENT    CHAR(1),
    SWIFT_ADDRESS   CHAR(12),
    DATE_OF_BIRTH   DATE,
    FULLNAME        CHAR(90),
    FIRSTNAME       CHAR(20),
    SURNAME         CHAR(70),
    FATHER_SURNAME  CHAR(40),
    MOTHER_SURNAME  VARCHAR(40),
    MEMBER_ID       INTEGER,
    MEMBER_STATUS   CHAR(10),
    BL_INDICATOR    CHAR(10),
    CHAMBER_ID      CHAR(10),
    EMPLOYEE_ID     CHAR(10),
    MEMBER_SUBFLAG  VARCHAR(40),
    CUST_STATUS     CHAR(1),
    constraint PK_CUSTSEARCH
        primary key (TRX_USR, TMSTAMP, ENTRY_SN)
);

create unique index IXU_SEARCHCUSTID
    on SEARCH_CUSTOMER (TRX_USR, TMSTAMP, CUST_ID);

