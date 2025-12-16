create table REP_DAILY_GL
(
    TRX_DATE      DATE     not null,
    ACC_UNIT      INTEGER  not null,
    ACC_MOVE_CURR INTEGER  not null,
    GL_BALANCE    DECIMAL(15, 2),
    GL_ACCOUNT    CHAR(21) not null,
    PRFT_SYSTEM   SMALLINT not null,
    constraint PK_REP74220
        primary key (TRX_DATE, PRFT_SYSTEM, GL_ACCOUNT, ACC_MOVE_CURR, ACC_UNIT)
);

