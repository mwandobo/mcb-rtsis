create table LOAN_STATEMENT_SPLIT
(
    CREATED_TIMESTAMP TIMESTAMP(6) not null,
    CUST_ID           INTEGER      not null,
    ACC_UNIT          INTEGER      not null,
    ACC_TYPE          SMALLINT     not null,
    ACC_SN            INTEGER      not null,
    SCHEDULED_DATE    DATE         not null,
    INSTANCE_NO       CHAR(5),
    PROCESSED_STATUS  CHAR(1),
    PRINT_FLG         CHAR(1),
    primary key (CREATED_TIMESTAMP, CUST_ID, ACC_UNIT, ACC_TYPE, ACC_SN, SCHEDULED_DATE)
);

