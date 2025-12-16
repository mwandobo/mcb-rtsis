create table WFS_KPI
(
    KPI_SN          DECIMAL(10) not null
        constraint PK_WFS_SCORE_KPI
            primary key,
    KPI_SN_LABEL    CHAR(40)    not null,
    KPI_ANALYSIS    VARCHAR(2048),
    KPI_DESCRIPTION VARCHAR(80),
    KPI_SUM         DECIMAL(18, 4),
    KPI_WORSE       DECIMAL(18, 4),
    KPI_BEST        DECIMAL(18, 4),
    KPI_MANUAL      CHAR(1),
    RULE_SYSTEM     SMALLINT,
    RULE_ID         DECIMAL(12),
    SCORE_SQL       VARCHAR(4000),
    CREATE_UNIT     INTEGER,
    CREATE_DATE     DATE,
    CREATE_USR      CHAR(8),
    CREATE_TMSTAMP  TIMESTAMP(6),
    UPDATE_UNIT     INTEGER,
    UPDATE_DATE     DATE,
    UPDATE_USR      CHAR(8),
    UPDATE_TMSTAMP  TIMESTAMP(6),
    KPI_FIELD_TYPE  CHAR(1)
);

