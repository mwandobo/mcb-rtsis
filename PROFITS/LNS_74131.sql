create table LNS_74131
(
    ACC_UNIT        INTEGER  not null,
    ACC_TYPE        SMALLINT not null,
    ACC_SN          INTEGER  not null,
    SCHEDULED_DATE  DATE     not null,
    CUST_ID         INTEGER,
    BATCH_DATE_FROM DATE,
    BATCH_DATE_TO   DATE,
    TMSTAMP         TIMESTAMP(6),
    constraint ILNS_74131
        primary key (ACC_UNIT, ACC_TYPE, ACC_SN, SCHEDULED_DATE)
);

