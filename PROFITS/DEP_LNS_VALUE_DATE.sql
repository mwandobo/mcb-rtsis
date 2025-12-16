create table DEP_LNS_VALUE_DATE
(
    CURRENCY_SHORTDESC CHAR(5)  not null,
    GL_ACC             CHAR(21) not null,
    BOOK_BALANCE       DECIMAL(15, 2),
    VALUE_BALANCE      DECIMAL(15, 2),
    DESCR              VARCHAR(50),
    constraint PVALUEDA
        primary key (GL_ACC, CURRENCY_SHORTDESC)
);

