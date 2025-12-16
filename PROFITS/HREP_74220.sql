create table HREP_74220
(
    GL_ACCOUNT         CHAR(21) not null,
    ACC_MOVE_CURR      INTEGER  not null,
    ACC_UNIT           INTEGER  not null,
    PRFT_SYSTEM        SMALLINT not null,
    GL_TRN_DATE        DATE     not null,
    GL_BALANCE         DECIMAL(18, 2),
    LEGER_BALANCE      DECIMAL(18, 2),
    CURR_ISO_CODE      CHAR(5),
    TP_GL_BALANCE      DECIMAL(18, 2),
    EXTERNAL_GLACCOUNT CHAR(21),
    MUTUAL             VARCHAR(1),
    constraint IXU_REP_056
        primary key (GL_ACCOUNT, ACC_MOVE_CURR, ACC_UNIT, PRFT_SYSTEM, GL_TRN_DATE)
);

