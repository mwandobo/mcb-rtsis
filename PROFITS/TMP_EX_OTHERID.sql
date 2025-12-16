create table TMP_EX_OTHERID
(
    CUST_ID                 INTEGER  not null
        constraint PK_CUST_OTHERID
            primary key,
    C_DIGIT                 SMALLINT not null,
    CUST_TYPE               CHAR(1),
    ID_NO                   VARCHAR(20),
    DESCRIPTION             VARCHAR(40),
    AFM_NO                  VARCHAR(20),
    SURNAME                 VARCHAR(70),
    FIRST_NAME              VARCHAR(20),
    FATHER_NAME             VARCHAR(20),
    MOTHER_NAME             VARCHAR(20),
    DATE_OF_BIRTH           VARCHAR(10),
    TEKE_FLG                VARCHAR(1),
    TRX_DATE                VARCHAR(10),
    TRX_UNIT                INTEGER,
    TRX_USER                VARCHAR(8),
    TRX_SN                  INTEGER,
    TRX_EMPLOYEE_ID         VARCHAR(8),
    TRX_EMPLOYEE_LAST_NAME  VARCHAR(20),
    TRX_EMPLOYEE_FIRST_NAME VARCHAR(20),
    ID_TYPE                 INTEGER
);

