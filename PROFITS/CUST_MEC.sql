create table CUST_MEC
(
    MEC_ID              DECIMAL(10) not null
        constraint PK_CUST_MEC
            primary key,
    CORPORATE_CUST_ID   INTEGER     not null,
    RESPONSIBLE_OFFICER CHAR(8)     not null,
    MEC_DESCR           VARCHAR(40),
    MEC_STATUS          CHAR(1),
    INSERT_UNIT         INTEGER,
    INSERT_USR          CHAR(8),
    INSERT_DT           DATE,
    INSERT_STAMP        TIMESTAMP(6),
    UPDATE_UNIT         INTEGER,
    UPDATE_USR          CHAR(8),
    UPDATE_DT           DATE,
    UPDATE_STAMP        TIMESTAMP(6),
    MEC_ANALYSIS        LONG VARCHAR(32700)
);

