create table MG_OTHER_ID
(
    FILE_NAME        CHAR(50)    not null,
    SN               DECIMAL(12) not null,
    FILE_DETAIL_ID   CHAR(2),
    OTHER_ID_SN      SMALLINT,
    ID_TYPE          CHAR(30),
    ID_NUM           CHAR(20),
    ISSUE_DATE       DATE,
    ISSUE_AUTH       CHAR(20),
    EXPIRY_DATE      DATE,
    ISSUE_COUNTRY    CHAR(30),
    ROW_PROCESS_DATE DATE,
    ROW_STATUS       CHAR(1),
    ROW_ERR_DESC     CHAR(80),
    CUSTOMER_CODE    CHAR(20),
    constraint PK_MG_OTHER_ID
        primary key (SN, FILE_NAME)
);

create unique index MG_OTHER_ID_PK
    on MG_OTHER_ID (FILE_NAME, SN);

