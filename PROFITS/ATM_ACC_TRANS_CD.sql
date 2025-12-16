create table ATM_ACC_TRANS_CD
(
    PROCCESSING_CODE CHAR(6) not null,
    TERM_LN          CHAR(4) not null,
    CARD_FIID        CHAR(4) not null,
    TRANSACTION_CODE CHAR(6),
    constraint IXU_ATM_038
        primary key (PROCCESSING_CODE, TERM_LN, CARD_FIID)
);

