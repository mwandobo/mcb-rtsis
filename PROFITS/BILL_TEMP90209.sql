create table BILL_TEMP90209
(
    SUM_AMOUNT         DECIMAL(15, 2),
    SUM_COUNT          INTEGER,
    OVERDUE_FLG        CHAR(1)  not null,
    CUST_ID            INTEGER  not null,
    C_DIGIT            SMALLINT,
    NORMAL_CHQ_COUNT   INTEGER,
    NORMAL_CHQ_AMOUNT  DECIMAL(15, 2),
    OVERDUE_CHQ_COUNT  INTEGER,
    OVERDUE_CHQ_AMOUNT DECIMAL(15, 2),
    BISS_FIRSTNAME     CHAR(20),
    BISS_TITLE         CHAR(70),
    UNIT_CODE          INTEGER,
    BISS_CODE          INTEGER  not null,
    BISS_CDIGIT        SMALLINT,
    BILL_COLL_ACC_NUMB CHAR(40) not null,
    BILL_COLL_ACC_CD   SMALLINT,
    constraint BILL_TEMP90209
        primary key (BILL_COLL_ACC_NUMB, BISS_CODE, CUST_ID)
);

