create table WFS_SCORECARD_ADJUST
(
    SCORECARD_SN        DECIMAL(10) not null,
    KPI_SN              DECIMAL(10) not null,
    KPI_VALUE_SN        DECIMAL(10) not null,
    PRIORITY            DECIMAL(5)  not null,
    SCORING_ADJ_AMOUNT  DECIMAL(5, 2),
    SCORING_ADJ_PERCENT DECIMAL(5, 2),
    CREATE_UNIT         DECIMAL(5),
    CREATE_DATE         DATE,
    CREATE_USR          CHAR(8),
    CREATE_TMSTAMP      TIMESTAMP(6),
    UPDATE_UNIT         DECIMAL(5),
    UPDATE_DATE         DATE,
    UPDATE_USR          CHAR(8),
    UPDATE_TMSTAMP      TIMESTAMP(6),
    ENTRY_STATUS        CHAR(1),
    constraint PK_WFE_SCORECARD_ADJUST
        primary key (KPI_VALUE_SN, KPI_SN, SCORECARD_SN)
);

