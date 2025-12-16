create table BASE_RATE_TEMP
(
    FK_GH_PAR_TYPE CHAR(5)      not null,
    FK_GD_SNUM     DECIMAL(5)   not null,
    DURATION       DECIMAL(4)   not null,
    DUR_UNIT       CHAR(1)      not null,
    FK_CURRENCY    DECIMAL(5)   not null,
    VALIDITY_DATE  TIMESTAMP(6) not null,
    TMSTAMP        TIMESTAMP(6) not null,
    BASE_RATE_PERC DECIMAL(9, 6),
    ENTRY_STATUS   CHAR(1),
    ERROR_DESC     CHAR(100)
);

