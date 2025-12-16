create table TERM_REP_73635
(
    DATE0         DATE,
    SERIAL_NUMBER INTEGER,
    PRODUCT_ID    INTEGER,
    COUNT0        INTEGER,
    AMOUNT        DECIMAL(15, 2),
    LOW           DECIMAL(15),
    HIGH          DECIMAL(15),
    DEPOSIT_TYPE  CHAR(1),
    PROD_DESC     VARCHAR(40)
);

create unique index IXU_TER_000
    on TERM_REP_73635 (DATE0, SERIAL_NUMBER);

