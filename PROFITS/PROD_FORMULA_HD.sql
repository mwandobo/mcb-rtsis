create table PROD_FORMULA_HD
(
    FORMULA_SN      DECIMAL(10) not null,
    FORMULA_DESC    VARCHAR(500),
    RESULT_FIELD    CHAR(40)    not null,
    ROUNDING_DIGITS SMALLINT,
    ENTRY_STATUS    CHAR(1),
    FK_PRODUCT_ID   INTEGER     not null,
    TMSTAMP         TIMESTAMP(6),
    constraint PK_PROD_FORMULA_HD
        primary key (FK_PRODUCT_ID, RESULT_FIELD, FORMULA_SN)
);

