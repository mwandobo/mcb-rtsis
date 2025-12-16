create table USER_DEFINED_TBL_DOC
(
    CUSTOMER_CODE     DECIMAL(10) not null,
    ACCOUNT_NUMBER    CHAR(40)    not null,
    PRFT_SYSTEM       SMALLINT    not null,
    RECORD_TYPE       CHAR(2)     not null,
    REFERENCE_NUMBER  VARCHAR(80) not null,
    PRFT_SYS          SMALLINT    not null,
    INSERT_UNIT       INTEGER,
    INSERT_DATE       DATE,
    INSERT_USR        CHAR(8),
    INSERT_TMSTAMP    TIMESTAMP(6),
    UPDATE_UNIT       INTEGER,
    UPDATE_DATE       DATE,
    UPDATE_USR        CHAR(8),
    UPDATE_TMSTAMP    TIMESTAMP(6),
    DOC_DESCR         VARCHAR(40),
    DOC_DETAILS       VARCHAR(500),
    LINKED_IMAGE      CHAR(1),
    LINKED_ATTACHMENT CHAR(1),
    ENTRY_STATUS      CHAR(1),
    CUST_ID           INTEGER,
    C_DIGIT           SMALLINT,
    constraint PK_UDF_DOC
        primary key (CUSTOMER_CODE, ACCOUNT_NUMBER, PRFT_SYSTEM, RECORD_TYPE, REFERENCE_NUMBER)
);

