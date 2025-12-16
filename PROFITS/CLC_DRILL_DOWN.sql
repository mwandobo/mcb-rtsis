create table CLC_DRILL_DOWN
(
    SHOW_ORDER       INTEGER not null
        constraint CLC_COLLECT_PK_71
            primary key,
    USE_ACCOUNT      CHAR(3),
    USE_AGREEMENT    CHAR(3),
    USE_FTM          CHAR(3),
    PRFT_SYSTEM      SMALLINT,
    TRAN_CODE        CHAR(8),
    SHOW_DESCRIPTION VARCHAR(80)
);

