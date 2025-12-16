create table BDG_FINAL_TRN
(
    FILENAME               CHAR(150),
    ERROR_DESCRIPTION      CHAR(80),
    TERMINAL_NO            CHAR(99),
    REMARKS                CHAR(80),
    FINILIZE_TMSTAMP       TIMESTAMP(6),
    CREATE_TMSTAMP         TIMESTAMP(6),
    TRX_USER_SN            INTEGER,
    TRX_USER               CHAR(8),
    TRX_UNIT               INTEGER,
    TRX_DATE               DATE,
    ENTRY_STATUS           CHAR(1),
    AMOUNT                 DECIMAL(15, 2) not null,
    FK_ISO_CURRENCYID_CURR SMALLINT,
    VERSION_ID             INTEGER        not null,
    YEAR0                  SMALLINT       not null,
    FK_UNITCODE            INTEGER        not null,
    FK_LINE_ID             CHAR(30)       not null,
    FK_PHASE_ID            CHAR(2)        not null,
    FK_STAGE_ID            INTEGER        not null,
    FK_PERIODYEAR0         SMALLINT       not null,
    FK_PERIOD_ID           VARCHAR(5)     not null,
    FK_STAGE_HD            CHAR(5)        not null,
    FK_STAGE_DT            INTEGER        not null,
    FK_CURRENCYID_CURRENCY INTEGER,
    constraint IXU_BDG_FINAL
        primary key (FK_LINE_ID, FK_PHASE_ID, FK_STAGE_ID, FK_PERIODYEAR0, FK_PERIOD_ID, FK_STAGE_HD, FK_STAGE_DT,
                     FK_UNITCODE, YEAR0, VERSION_ID)
);

create unique index I0000327
    on BDG_FINAL_TRN (FK_PHASE_ID, FK_STAGE_ID);

create unique index I0000331
    on BDG_FINAL_TRN (FK_PERIODYEAR0, FK_PERIOD_ID);

create unique index I0000332
    on BDG_FINAL_TRN (FK_STAGE_HD, FK_STAGE_DT);

create unique index I0000334
    on BDG_FINAL_TRN (FK_CURRENCYID_CURRENCY);

create unique index I0010312
    on BDG_FINAL_TRN (FK_UNITCODE);

create unique index I0010329
    on BDG_FINAL_TRN (FK_LINE_ID);

