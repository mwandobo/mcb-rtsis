create table EXT_TASK_ASK
(
    TRX_TASK_ID       DECIMAL(10)  not null,
    TRX_ACC_NUMBER    CHAR(40)     not null,
    TRX_CUST_ID       INTEGER      not null,
    TRX_DATE          DATE         not null,
    TRX_UNIT          INTEGER      not null,
    TRX_USER          CHAR(8)      not null,
    TRX_TMSTAMP       TIMESTAMP(6) not null,
    TRX_COUNTER       DECIMAL(12)  not null,
    SENT_TASK_ID      DECIMAL(10),
    SENT_TASK_DT      DATE,
    SENT_TASK_SN      DECIMAL(13),
    SENT_TASK_TMSTAMP TIMESTAMP(6),
    SENT_TASK_SYSTEM  SMALLINT,
    SENT_ACC_NUMBER   CHAR(40),
    SENT_CUST_ID      INTEGER,
    CURRENT_STATUS    VARCHAR(10),
    EXT_MESSAGE       VARCHAR(4000),
    SEND_BY_PROFITS   CHAR(1),
    constraint PK_EXTTSK6
        primary key (TRX_COUNTER, TRX_TMSTAMP, TRX_USER, TRX_UNIT, TRX_DATE, TRX_CUST_ID, TRX_ACC_NUMBER, TRX_TASK_ID)
);

