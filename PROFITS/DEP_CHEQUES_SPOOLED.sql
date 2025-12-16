create table DEP_CHEQUES_SPOOLED
(
    TRX_DATE          DATE         not null,
    TIMESTMP          TIMESTAMP(6) not null,
    SERIAL_NUMBER     DECIMAL(10)  not null,
    PRINTED_FLAG      CHAR(1)      not null,
    BOOKS_NUMBER      DECIMAL(5)   not null,
    BOOK_PAGES        DECIMAL(3)   not null,
    ORDERING_UNIT     DECIMAL(5)   not null,
    ACCOUNT_NUMBER    CHAR(40)     not null,
    CHEQUE_FST_NUMBER DECIMAL(10)  not null,
    LAST_NUMBER       DECIMAL(10)  not null,
    DEPOSIT_TYPE      CHAR(2)      not null,
    CHEQUES_TYPE      CHAR(7)      not null,
    CUSTOMER_NAME     CHAR(120)    not null,
    UNIT1             DECIMAL(5)   not null,
    UNIT2             DECIMAL(5)   not null,
    CHEQUE_CD_2       DECIMAL(2),
    SHORT_DESCR       CHAR(5),
    CUST_TYPE         CHAR(1)
);

create unique index PK_INDEX
    on DEP_CHEQUES_SPOOLED (ACCOUNT_NUMBER, CHEQUE_FST_NUMBER, LAST_NUMBER, TRX_DATE, SERIAL_NUMBER);

