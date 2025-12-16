create table HREP_74220_DET
(
    GL_TRN_DATE   DATE        not null,
    SERIALNUM     DECIMAL(15) not null,
    ACC_UNIT      INTEGER     not null,
    ACC_MOVE_CURR INTEGER     not null,
    GL_ACCOUNT    CHAR(21)    not null,
    PRFT_SYSTEM   SMALLINT    not null,
    PROF_ACCOUNT  CHAR(40)    not null,
    ORIGIN_TYPE   CHAR(1)     not null,
    CHARGE_CODE   INTEGER     not null,
    ORIGIN_ID     CHAR(2)     not null,
    GL_BALANCE    DECIMAL(18, 2),
    ARTICLE_SN    DECIMAL(15),
    CASH_TILL_NO  DECIMAL(5),
    constraint PKHREP74220DET
        primary key (GL_TRN_DATE, SERIALNUM, PRFT_SYSTEM, ACC_MOVE_CURR, ACC_UNIT, PROF_ACCOUNT, ORIGIN_ID, ORIGIN_TYPE,
                     CHARGE_CODE)
);

create unique index IDX_HREP74220_HKEEP
    on HREP_74220_DET (GL_TRN_DATE);

