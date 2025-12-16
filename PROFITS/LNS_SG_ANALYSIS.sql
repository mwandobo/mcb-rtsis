create table LNS_SG_ANALYSIS
(
    ACC_UNIT        SMALLINT     not null,
    ACC_TYPE        SMALLINT     not null,
    ACC_SN          INTEGER      not null,
    TMSTAMP         TIMESTAMP(6) not null,
    TRX_INTERNAL_SN SMALLINT     not null,
    TRX_DATE        DATE,
    RL_NRM_INT_AMN  DECIMAL(15, 2),
    RL_PNL_INT_AMN  DECIMAL(15, 2),
    TRX_CODE        INTEGER,
    ID_JUSTIFIC     INTEGER,
    GL_ACCOUNT      CHAR(21),
    constraint IXU_REP_020
        primary key (ACC_UNIT, ACC_TYPE, ACC_SN, TMSTAMP, TRX_INTERNAL_SN)
);

