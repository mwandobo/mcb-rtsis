create table TAX_SCALE
(
    FK_TAXID_TAX       INTEGER,
    FK_CURRENCYID_CURR INTEGER,
    VALIDITY_DATE      DATE,
    SCALE_ID           SMALLINT,
    PERCENTAGE         DECIMAL(8, 4),
    RESULT_AMOUNT      DECIMAL(15, 2),
    TO_AMOUNT          DECIMAL(15, 2),
    FROM_AMOUNT        DECIMAL(15, 2),
    TAX                DECIMAL(15, 2),
    MIN_TAX            DECIMAL(15, 2),
    MAX_TAX            DECIMAL(15, 2),
    TMSTAMP            TIMESTAMP(6)
);

create unique index IXU_TAX_000
    on TAX_SCALE (FK_TAXID_TAX, FK_CURRENCYID_CURR, VALIDITY_DATE, SCALE_ID);

