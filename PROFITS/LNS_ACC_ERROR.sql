create table LNS_ACC_ERROR
(
    TRX_DATE         DATE,
    PROF_ACC_NUMBER  CHAR(40),
    ACC_TYPE         SMALLINT,
    PROF_ACC_CD      SMALLINT,
    ACC_CD           SMALLINT,
    UNITCODE         INTEGER,
    ID_JUSTIFIC      INTEGER,
    TRX_CODE         INTEGER,
    TRX_UNIT         INTEGER,
    ID_PRODUCT       INTEGER,
    MONITORING_UNIT  INTEGER,
    ACC_SN           INTEGER,
    TRX_USER         CHAR(8),
    DCD_ACTION       CHAR(20),
    DCD_PROCEDURE    CHAR(40),
    DCD_ENTITY       CHAR(40),
    DCD_ACTION_BLOCK CHAR(40),
    DCD_KEYVAL2      CHAR(80),
    DCD_KEYVAL4      CHAR(80),
    DCD_KEYVAL1      CHAR(80),
    DCD_KEYVAL5      CHAR(80),
    DCD_COMMENT      CHAR(80),
    DCD_KEYVAL3      CHAR(80),
    DCD_KEYVAL6      CHAR(80),
    ERROR_MSG        CHAR(80),
    TMSTAMP          TIMESTAMP(6) not null
);

create unique index IXU_LNS_015
    on LNS_ACC_ERROR (TRX_DATE, PROF_ACC_NUMBER, TMSTAMP);

