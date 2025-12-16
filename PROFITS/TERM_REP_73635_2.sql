create table TERM_REP_73635_2
(
    PRODUCT_ID    INTEGER not null,
    DATE0         DATE    not null,
    SERIAL_NUMBER INTEGER not null,
    COUNT0        INTEGER,
    LOW           DECIMAL(15),
    HIGH          DECIMAL(15),
    AMOUNT        DECIMAL(15, 2),
    DEPOSIT_TYPE  CHAR(1),
    PROD_DESC     VARCHAR(40),
    constraint IXU_REP_190
        primary key (PRODUCT_ID, DATE0, SERIAL_NUMBER)
);

