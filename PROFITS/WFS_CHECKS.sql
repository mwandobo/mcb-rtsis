create table WFS_CHECKS
(
    CHECK_SN           DECIMAL(10) not null
        constraint PK_WFS_CHECKS
            primary key,
    CHECK_SN_LABEL     CHAR(40)    not null,
    CHECK_DESCRIPTION  VARCHAR(80),
    CHECK_ANALYSIS     VARCHAR(2048),
    CHECK_MANDATORY    CHAR(1),
    CHECK_PRE_APPROVAL CHAR(1),
    CHECK_PRE_FINALIZE CHAR(1),
    CREATE_UNIT        INTEGER,
    CREATE_DATE        DATE,
    CREATE_USR         CHAR(8),
    CREATE_TMSTAMP     TIMESTAMP(6),
    UPDATE_UNIT        INTEGER,
    UPDATE_DATE        DATE,
    UPDATE_USR         CHAR(8),
    UPDATE_TMSTAMP     TIMESTAMP(6)
);

