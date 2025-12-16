create table HTAX_SCALE
(
    FK_CURRENCYID_CURR INTEGER  not null,
    FK_TAXID_TAX       INTEGER  not null,
    SNUM               SMALLINT not null,
    VALIDITY_DATE      DATE     not null,
    ID_CURRENCY        INTEGER,
    ID_TAX             INTEGER,
    CALC_METHOD        CHAR(1),
    MIN_AMOUNT         DECIMAL(15, 2),
    MIN_TAX            DECIMAL(15, 2),
    MAX_TAX            DECIMAL(15, 2),
    CONTINUATION       CHAR(1),
    TAX1               DECIMAL(15, 2),
    PERCENTAGE1        DECIMAL(8, 4),
    AMOUNT1            DECIMAL(15, 2),
    TAX2               DECIMAL(15, 2),
    PERCENTAGE2        DECIMAL(8, 4),
    AMOUNT2            DECIMAL(15, 2),
    TAX3               DECIMAL(15, 2),
    PERCENTAGE3        DECIMAL(8, 4),
    AMOUNT3            DECIMAL(15, 2),
    TAX4               DECIMAL(15, 2),
    PERCENTAGE4        DECIMAL(8, 4),
    AMOUNT4            DECIMAL(15, 2),
    TAX5               DECIMAL(15, 2),
    PERCENTAGE5        DECIMAL(8, 4),
    AMOUNT5            DECIMAL(15, 2),
    TAX6               DECIMAL(15, 2),
    PERCENTAGE6        DECIMAL(8, 4),
    AMOUNT6            DECIMAL(15, 2),
    TAX7               DECIMAL(15, 2),
    PERCENTAGE7        DECIMAL(8, 4),
    AMOUNT7            DECIMAL(15, 2),
    TAX8               DECIMAL(15, 2),
    PERCENTAGE8        DECIMAL(8, 4),
    AMOUNT8            DECIMAL(15, 2),
    TAX9               DECIMAL(15, 2),
    PERCENTAGE9        DECIMAL(8, 4),
    AMOUNT9            DECIMAL(15, 2),
    TAX0               DECIMAL(15, 2),
    PERCENTAGE0        DECIMAL(8, 4),
    AMOUNT0            DECIMAL(15, 2),
    TMSTAMP            TIMESTAMP(6),
    ENTRY_STATUS       CHAR(1),
    constraint PK_HTAX_SCALE
        primary key (FK_CURRENCYID_CURR, FK_TAXID_TAX, SNUM, VALIDITY_DATE)
);

create unique index I0000147
    on HTAX_SCALE (FK_CURRENCYID_CURR);

create unique index I0000153
    on HTAX_SCALE (FK_TAXID_TAX);

