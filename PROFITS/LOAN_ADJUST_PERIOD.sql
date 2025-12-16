create table LOAN_ADJUST_PERIOD
(
    LNS_OPEN_UNIT        INTEGER  not null,
    LNS_TYPE             SMALLINT not null,
    LNS_SN               INTEGER  not null,
    RECORD_SN            SMALLINT not null,
    ADJ_REQ_INST_FROM    SMALLINT,
    ADJ_REQ_INST_TO      SMALLINT,
    ADJ_FIXED_INST_AMN   DECIMAL(15, 2),
    TMSTAMP              TIMESTAMP(6),
    TRX_DATE             DATE,
    TRX_UNIT             INTEGER,
    TRX_USR              CHAR(8),
    ZERO_INT_FLG         CHAR(1),
    ENTRY_STATUS         CHAR(1),
    LAST_UPDATE_USR      CHAR(8),
    LAST_UPDATE_UNIT     INTEGER,
    LAST_UPDATE_DATE     DATE,
    LST_UPD_TMSTAMP      TIMESTAMP(6),
    MAX_INST_CONTROL_FLG CHAR(1)
);

create unique index PK_LNS_ADJ_PER
    on LOAN_ADJUST_PERIOD (LNS_OPEN_UNIT, LNS_TYPE, LNS_SN, RECORD_SN);

