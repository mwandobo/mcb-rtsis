create table PASS_PARAMS
(
    BANK_CODE   SMALLINT,
    MAX         SMALLINT,
    MIN         SMALLINT,
    HISTORY_NUM INTEGER,
    NUMS        CHAR(1),
    CASESEN     CHAR(1),
    UPCAPS      CHAR(1),
    LOWCAPS     CHAR(1),
    CHARSTR     CHAR(1),
    NUMSTR      CHAR(1),
    SYMBS       CHAR(1)
);

create unique index PK_PASS_PARAM
    on PASS_PARAMS (BANK_CODE);

