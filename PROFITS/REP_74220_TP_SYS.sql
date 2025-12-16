create table REP_74220_TP_SYS
(
    PRFT_SYSTEM   SMALLINT not null,
    ACC_UNIT      INTEGER  not null,
    ACC_MOVE_CURR INTEGER  not null,
    GL_ACCOUNT    CHAR(21) not null,
    TP_GL_BALANCE DECIMAL(15, 2),
    GL_TRN_DATE   DATE     not null,
    TIMESTMP      DATE,
    CURR_ISO_CODE CHAR(5),
    constraint PK_IXU_REP_056
        primary key (PRFT_SYSTEM, GL_ACCOUNT, ACC_MOVE_CURR, ACC_UNIT, GL_TRN_DATE)
);

