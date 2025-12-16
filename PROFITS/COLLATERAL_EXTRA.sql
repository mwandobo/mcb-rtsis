create table COLLATERAL_EXTRA
(
    DTL_RECORD_TYPE CHAR(2) not null
        constraint PK_CTBLDTDESC
            primary key,
    SHOW_ORDER      SMALLINT,
    RECORD_DESC     VARCHAR(40)
);

