create table REP_74220_TP_GL
(
    ACC_UNIT      INTEGER  not null,
    ACC_MOVE_CURR INTEGER  not null,
    GL_ACCOUNT    CHAR(21) not null,
    TP_GL_BALANCE DECIMAL(15, 2),
    GL_TRN_DATE   DATE     not null,
    TIMESTMP      TIMESTAMP(6),
    constraint IXU_REP_74220_TP_GL
        primary key (GL_ACCOUNT, ACC_MOVE_CURR, ACC_UNIT, GL_TRN_DATE)
);

