create table CONSOL_CNTY_AMOUNT
(
    COUNTRY CHAR(20) not null
        constraint IXU_DEP_155
            primary key,
    COUNT0  INTEGER,
    AMOUNT  DECIMAL(15, 2)
);

