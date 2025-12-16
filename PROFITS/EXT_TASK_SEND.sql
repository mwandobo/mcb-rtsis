create table EXT_TASK_SEND
(
    EXT_TASK_ID          DECIMAL(10)  not null,
    EXT_TASK_DT          DATE         not null,
    EXT_TASK_SN          DECIMAL(13)  not null,
    EXT_TASK_TMSTAMP     TIMESTAMP(6) not null,
    EXT_TASK_SYSTEM      SMALLINT     not null,
    ACCOUNT_NUMBER       CHAR(40)     not null,
    CUST_ID              INTEGER      not null,
    EXT_TASK_DESCRIPTION CHAR(80),
    C_DIGIT              SMALLINT,
    ACCOUNT_CD           SMALLINT,
    SEND_EMAIL           CHAR(1),
    SEND_SMS             CHAR(1),
    EXT_TASK_STATUS      CHAR(1),
    constraint PK_EXTTSK2
        primary key (CUST_ID, ACCOUNT_NUMBER, EXT_TASK_SYSTEM, EXT_TASK_TMSTAMP, EXT_TASK_SN, EXT_TASK_DT, EXT_TASK_ID)
);

