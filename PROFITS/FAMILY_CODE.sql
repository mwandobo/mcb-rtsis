create table FAMILY_CODE
(
    FAMILY_CODE   DECIMAL(11) not null
        constraint IXU_CP_064
            primary key,
    STATUS        SMALLINT,
    CUST_CD       SMALLINT,
    ACC_DIGIT     SMALLINT,
    DEB_JUSTIFIC  INTEGER,
    JUSTIFIC      INTEGER,
    CUSTOMER_ID   INTEGER,
    PID           DECIMAL(11),
    TEL           CHAR(12),
    DEBIT_ACCOUNT CHAR(40),
    JAST_DESC     CHAR(40),
    FULL_NAME     CHAR(100)
);

