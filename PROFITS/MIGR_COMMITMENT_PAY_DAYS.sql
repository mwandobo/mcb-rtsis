create table MIGR_COMMITMENT_PAY_DAYS
(
    FILE_NAME            CHAR(50)    not null,
    SERIAL_NO            DECIMAL(10) not null,
    ROW_STATUS           SMALLINT,
    ROW_ERR_DESC         CHAR(80),
    ROW_PROCESS_DATE     DATE,
    ROW_TMSTAMP          TIMESTAMP(6),
    UTF_TEXT1            CHAR(80),
    UTF_TEXT2            CHAR(80),
    UTF_DATE1            DATE,
    UTF_DATE2            DATE,
    UTF_NUM1             DECIMAL(15),
    UTF_NUM2             DECIMAL(15),
    OTHER_SYS_IDENTIFIER CHAR(20),
    PAY_DAYS             SMALLINT,
    ACTIVATION_DATE      DATE,
    constraint PK_MIGR_COMM_PAY_DAYS
        primary key (SERIAL_NO, FILE_NAME)
);

