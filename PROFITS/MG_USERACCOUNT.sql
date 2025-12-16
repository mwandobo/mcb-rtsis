create table MG_USERACCOUNT
(
    FILE_NAME        CHAR(50) not null,
    SERIAL_NO        INTEGER  not null,
    USERID           INTEGER,
    ACCOUNTNUMBER    CHAR(50),
    DESCRIPTION      CHAR(50),
    RELATIONID       SMALLINT,
    ROW_STATUS       SMALLINT,
    ROW_TMSTAMP      DATE,
    ROW_ERR_DESC     CHAR(80),
    ROW_PROCESS_DATE DATE,
    constraint PK_MG_USERACCOUNT
        primary key (SERIAL_NO, FILE_NAME)
);

