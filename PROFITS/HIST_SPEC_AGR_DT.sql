create table HIST_SPEC_AGR_DT
(
    SERIAL_NUM        INTEGER not null,
    TRX_DATE          DATE    not null,
    TRX_USER_SN       INTEGER not null,
    TRX_UNIT          INTEGER not null,
    TRX_USER          CHAR(8) not null,
    VALUE_DATE_SPREAD SMALLINT,
    AVAIL_DATE_SPREAD SMALLINT,
    TRX_ID            INTEGER,
    JUSTIF_ID         INTEGER,
    PROD_ID           INTEGER,
    CHARGES_DISCOUNT  DECIMAL(8, 4),
    COMM_DISCOUNT     DECIMAL(8, 4),
    TMSTAMP           TIMESTAMP(6),
    CATEGORY_CODE     CHAR(8),
    PARAMETER_TYPE    CHAR(10),
    constraint IXU_CIS_173
        primary key (SERIAL_NUM, TRX_DATE, TRX_USER_SN, TRX_UNIT, TRX_USER)
);

