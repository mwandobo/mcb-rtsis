create table CUST_TRAN_INFO_3
(
    CUST_ID              INTEGER      not null,
    C_DIGIT              SMALLINT,
    TMSTAMP              TIMESTAMP(6) not null,
    TRX_USER             CHAR(8),
    SN                   INTEGER      not null,
    TRX_UNIT             INTEGER,
    TRX_CODE             INTEGER,
    TRX_DATE             DATE,
    TRX_USR_SN           INTEGER,
    C_ID_NO              CHAR(20),
    P_ID_NO              CHAR(20),
    C_ISSUE_DATE         DATE,
    P_ISSUE_DATE         DATE,
    C_EXPIRY_DATE_OTH_ID DATE,
    P_EXPIRY_DATE_OTH_ID DATE,
    C_ISSUE_AUTHORITY    CHAR(30),
    P_ISSUE_AUTHORITY    CHAR(30),
    C_AFM_NO             CHAR(20),
    P_AFM_NO             CHAR(20),
    C_ID_TAX_OFFICE      SMALLINT,
    P_ID_TAX_OFFICE      SMALLINT,
    C_AFM_ISSUE_COUNTRY  CHAR(10),
    P_AFM_ISSUE_COUNTRY  CHAR(10),
    C_OTHER_ID_TYPE      INTEGER,
    P_OTHER_ID_TYPE      INTEGER,
    C_OTHER_ID_COUNTRY   CHAR(10),
    P_OTHER_ID_COUNTRY   CHAR(10),
    constraint PK_CUST_TRAN_INFO_3
        primary key (TMSTAMP, CUST_ID, SN)
);

