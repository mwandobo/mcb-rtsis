create table INTER_REMIT_HD_PLS
(
    REMITT_NUMB_PL    DECIMAL(10) not null
        constraint IXU_FX_015
            primary key,
    FK_UNITCODE       INTEGER,
    FK0UNITCODE       INTEGER,
    AM                DECIMAL(10),
    COMPLETE_DATE     TIMESTAMP(6),
    SEND_DATE         TIMESTAMP(6),
    ORIG_DATE         TIMESTAMP(6),
    LAST_UPD_TMP      TIMESTAMP(6),
    REM_STATUS        CHAR(1),
    ENTRY_STATUS      CHAR(1),
    RUNNER_ID         CHAR(8),
    LAST_USR_UPD      CHAR(8),
    RUNNER_LAST_NAME  CHAR(20),
    CHIP_ID           CHAR(20),
    RUNNER_FIRST_NAME CHAR(20),
    ENTRY_COMMENTS    CHAR(40)
);

