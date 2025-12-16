create table MG_COS_APPL_DET
(
    FILE_NAME          CHAR(50) not null,
    SERIAL_NO          INTEGER  not null,
    OLD_APPLICATION_ID DECIMAL(11),
    SHARE_ID           DECIMAL(10),
    ROW_STATUS         SMALLINT,
    ROW_ERR_DESC       CHAR(80),
    ROW_PROCESS_DATE   DATE,
    UTF_TEXT1          CHAR(80),
    UTF_TEXT2          CHAR(80),
    UTF_DATE1          DATE,
    UTF_DATE2          DATE,
    UTF_NUM1           DECIMAL(15, 2),
    UTF_NUM2           DECIMAL(15, 2),
    constraint PK_MG_COS_APPL_DET
        primary key (FILE_NAME, SERIAL_NO)
);

create unique index IX_OLD_APPLICATION_ID
    on MG_COS_APPL_DET (OLD_APPLICATION_ID);

