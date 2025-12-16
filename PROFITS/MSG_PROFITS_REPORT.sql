create table MSG_PROFITS_REPORT
(
    ID                DECIMAL(12) not null
        constraint IXM_PRR_000
            primary key,
    FROM_SYSTEM       VARCHAR(50) not null,
    RECIPIENT         VARCHAR(50) not null,
    MSG_TYPE          VARCHAR(50) not null,
    MSG_SENT_STATUS   VARCHAR(20),
    MSG_SENT_DATE     DATE,
    MSG_SUBJECT       VARCHAR(100),
    MSG_SHORT_MESSAGE CLOB(1048576),
    CUSTOMER_ID       VARCHAR(50)
);

