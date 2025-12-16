create table TMP_EXCUST
(
    CUST_ID       INTEGER  not null
        constraint PK_CUSTID
            primary key,
    C_DIGIT       SMALLINT not null,
    CUST_TYPE     VARCHAR(1),
    ID_NO         VARCHAR(20),
    DESCRIPTION   VARCHAR(40),
    AFM_NO        VARCHAR(20),
    SURNAME       VARCHAR(70),
    FIRST_NAME    VARCHAR(20),
    FATHER_NAME   VARCHAR(20),
    MOTHER_NAME   VARCHAR(20),
    DATE_OF_BIRTH VARCHAR(10),
    TEKE_FLG      VARCHAR(1)
);

