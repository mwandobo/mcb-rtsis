create table PASS_PARAMS_HISTORY
(
    TRX_USER      CHAR(8)  not null,
    TRX_UNIT      INTEGER  not null,
    TRX_DATE      DATE     not null,
    TRX_USR_SN    INTEGER  not null,
    GRP_SUBSCRIPT SMALLINT not null,
    MIN_BF        SMALLINT not null,
    MIN_AF        SMALLINT not null,
    MAX_BF        SMALLINT not null,
    MAX_AF        SMALLINT not null,
    CHARSTR_BF    CHAR(1),
    CHARSTR_AF    CHAR(1),
    NUMSTR_BF     CHAR(1),
    NUMSTR_AF     CHAR(1),
    NUMS_BF       CHAR(1),
    NUMS_AF       CHAR(1),
    SYMBS_BF      CHAR(1),
    SYMBS_AF      CHAR(1),
    LOWCAPS_BF    CHAR(1),
    LOWCAPS_AF    CHAR(1),
    UPCAPS_BF     CHAR(1),
    UPCAPS_AF     CHAR(1),
    CASESEN_BF    CHAR(1),
    CASESEN_AF    CHAR(1),
    constraint PK_PASS_PARAMS_HISTORY
        primary key (GRP_SUBSCRIPT, TRX_USR_SN, TRX_DATE, TRX_UNIT, TRX_USER)
);

