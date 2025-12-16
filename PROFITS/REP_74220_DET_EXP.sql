create table REP_74220_DET_EXP
(
    SERIALNUM     DECIMAL(15) not null,
    ACC_UNIT      INTEGER     not null,
    ACC_MOVE_CURR INTEGER     not null,
    GL_ACCOUNT    CHAR(21)    not null,
    PRFT_SYSTEM   SMALLINT    not null,
    PROF_ACCOUNT  CHAR(40)    not null,
    ORIGIN_TYPE   CHAR(1)     not null,
    CHARGE_CODE   INTEGER     not null,
    ORIGIN_ID     CHAR(2)     not null,
    GL_BALANCE    DECIMAL(15, 2),
    ERRORMESSAGE  VARCHAR(1000),
    TMSTAMP       TIMESTAMP(6)
);

create unique index PKREP74220DETEXP
    on REP_74220_DET_EXP (SERIALNUM, PRFT_SYSTEM, ACC_MOVE_CURR, ACC_UNIT, PROF_ACCOUNT, ORIGIN_ID, ORIGIN_TYPE,
                          CHARGE_CODE, TMSTAMP);

