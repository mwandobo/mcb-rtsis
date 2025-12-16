create table MG_COS_APPLICAT
(
    FILE_NAME          CHAR(50) not null,
    SERIAL_NO          INTEGER  not null,
    ROW_STATUS         SMALLINT,
    BLOCKED_TYPE       SMALLINT,
    APPL_TYPE          SMALLINT,
    MEMBER_ID          INTEGER,
    NUMBER_OF_SHARES   DECIMAL(10),
    NEW_APPLICATION_ID DECIMAL(11),
    OLD_APPLICATION_ID DECIMAL(11),
    APPL_SHARE_PRICE   DECIMAL(15, 2),
    APPL_AMOUNT        DECIMAL(15, 2),
    UTF_NUM1           DECIMAL(15, 2),
    UTF_NUM2           DECIMAL(15, 2),
    ROW_PROCESS_DATE   DATE,
    UTF_DATE2          DATE,
    EXPIRATION_DATE    DATE,
    CREATION_DATE      DATE,
    UTF_DATE1          DATE,
    ROW_TMSTAMP        TIMESTAMP(6),
    ACCOUNT_NUMBER     CHAR(40),
    UTF_TEXT1          CHAR(80),
    UTF_TEXT2          CHAR(80),
    COMMENT_3          CHAR(80),
    ROW_ERR_DESC       CHAR(80),
    COMMENT_1          CHAR(80),
    COMMENT_2          CHAR(80),
    UTF_TEXT3          CHAR(80),
    constraint IXU_MIG_015
        primary key (FILE_NAME, SERIAL_NO)
);

