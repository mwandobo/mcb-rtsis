create table DEP_CHEQUES_CYP
(
    TRX_DATE          DATE         not null,
    TIMESTMP          TIMESTAMP(6) not null,
    SERIAL_NUMBER     DECIMAL(10)  not null
        constraint DEP_CHEQUES_CYP_PK
            primary key,
    PRINTED_FLG       CHAR(1)      not null,
    BOOKS_NUMBER      INTEGER      not null,
    BOOK_PAGES        SMALLINT     not null,
    ORDERING_UNIT     INTEGER      not null,
    ACCOUNT_NUMBER    CHAR(40)     not null,
    CHEQUE_FST_NUMBER DECIMAL(10)  not null,
    LAST_NUMBER       DECIMAL(10)  not null,
    DEPOSIT_TYPE      CHAR(2)      not null,
    CHEQUES_TYPE      CHAR(7)      not null,
    CUSTOMER_NAME     CHAR(120)    not null,
    UNIT1             INTEGER      not null,
    UNIT2             INTEGER      not null,
    CHEQUE_CD_2       SMALLINT,
    SHORT_DESCR       CHAR(5),
    CUST_TYPE         CHAR(1),
    CUST_ID           CHAR(11),
    ADDRESS_1         VARCHAR(30),
    ADDRESS_2         VARCHAR(30),
    ADDRESS_3         CHAR(30),
    UNIT_NAME         VARCHAR(30),
    BANK_DRAFT_FLAG   CHAR(2),
    BANK_NAME         VARCHAR(39)
);

