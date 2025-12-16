create table TEMP_TKA_REP
(
    REP_ROW         SMALLINT,
    TKA_NUMBER      INTEGER,
    TKA_TOT_PER_CAT DECIMAL(15, 2),
    TKA_VALUE       DECIMAL(15, 2),
    TKA_CATEGORY    CHAR(2)
);

create unique index SYS_C002000605
    on TEMP_TKA_REP (REP_ROW);

