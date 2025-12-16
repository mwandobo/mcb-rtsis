create table TEMP_SPEC_AGR
(
    SERIAL_NUM        INTEGER      not null,
    TMSTAMP           TIMESTAMP(6) not null,
    VALUE_DATE_SPREAD SMALLINT,
    AVAIL_DATE_SPREAD SMALLINT,
    TRX_ID            INTEGER,
    PROD_ID           INTEGER,
    JUSTIF_ID         INTEGER,
    COMM_DISCOUNT     DECIMAL(8, 4),
    CHARGES_DISCOUNT  DECIMAL(8, 4),
    constraint IXU_REP_092
        primary key (SERIAL_NUM, TMSTAMP)
);

