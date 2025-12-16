create table ATM_DAILY_TOTALS
(
    TRAN_DATE      DATE     not null,
    CARD_LN        CHAR(4)  not null,
    TERM_ID        CHAR(16) not null,
    TRAN_CODE      CHAR(6)  not null,
    RESP_CODE      CHAR(3)  not null,
    MESSAGE_TYPE   CHAR(4)  not null,
    TRANS_COUNTER  DECIMAL(10),
    SUM_TRANS_AMNT DECIMAL(15, 2),
    BRANCH_ID      CHAR(4),
    TERM_LN        CHAR(4),
    TERM_LOCATION  CHAR(25),
    constraint IXU_ATM_033
        primary key (TRAN_DATE, CARD_LN, TERM_ID, TRAN_CODE, RESP_CODE, MESSAGE_TYPE)
);

