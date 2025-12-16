create table VAT_DETAIL
(
    VAT_DETAIL_SN      DECIMAL(5) not null,
    FK_ID_VAT          DECIMAL(5) not null,
    FK_CURRENCYID_CURR DECIMAL(5),
    VALIDITY_DATE      DATE       not null,
    FROM_AMOUNT        DECIMAL(15, 2),
    TO_AMOUNT          DECIMAL(15, 2),
    PERCENTAGE         DECIMAL(8, 4),
    TMSTAMP            TIMESTAMP(6),
    ENTRY_STATUS       CHAR(1),
    constraint VAT_DETAIL_PK
        primary key (FK_ID_VAT, VALIDITY_DATE)
);

