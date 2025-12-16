create table REP_74220_FOREX
(
    PRFT_SYSTEM   SMALLINT not null,
    ACC_UNIT      INTEGER  not null,
    ACC_MOVE_CURR INTEGER  not null,
    GL_ACCOUNT    CHAR(21) not null,
    TP_GL_BALANCE DECIMAL(15, 2),
    GL_TRN_DATE   DATE     not null,
    TIMESTMP      DATE,
    CURR_ISO_CODE CHAR(5)
);

