create table TMP_EX_OTHERID2
(
    TRX_DATE                DATE,
    TRX_UNIT                INTEGER,
    TRX_USER                CHAR(8),
    TRX_USR_SN              INTEGER,
    TRX_EMPLOYEE_ID         VARCHAR(8),
    TRX_EMPLOYEE_LAST_NAME  VARCHAR(20),
    TRX_EMPLOYEE_FIRST_NAME VARCHAR(20),
    TRX_CODE                INTEGER,
    CUST_ID                 INTEGER  not null,
    C_DIGIT                 SMALLINT not null,
    CUST_TYPE               VARCHAR(1),
    ID_NO                   VARCHAR(20),
    DESCRIPTION             VARCHAR(40),
    AFM_NO                  VARCHAR(20),
    SURNAME                 VARCHAR(70),
    FIRST_NAME              VARCHAR(20),
    FATHER_NAME             VARCHAR(20),
    MOTHER_NAME             VARCHAR(20),
    DATE_OF_BIRTH           VARCHAR(10),
    TEKE_FLG                VARCHAR(1),
    ID_TYPE                 INTEGER,
    FOUND_FLG               VARCHAR(10),
    TMSTAMP                 DATE
);

create unique index PK_CUSTOTHER
    on TMP_EX_OTHERID2 (TMSTAMP, CUST_ID);

