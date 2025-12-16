create table CUST_VIDEO
(
    CUST_ID       INTEGER not null,
    SERIAL_NUM    INTEGER not null,
    ACTIVE        CHAR(1),
    PATH          VARCHAR(1024),
    DESCRIPTION   CHAR(200),
    TMSTAMP       TIMESTAMP(6),
    LIVENESS_PERC SMALLINT,
    constraint PK_CUST_VIDEO
        primary key (SERIAL_NUM, CUST_ID)
);

