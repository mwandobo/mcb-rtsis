create table GLG_OFFICIAL_75209
(
    YEAR         SMALLINT not null,
    PERIOD
    SMALLINT
    not null,
    JOURNAL      CHAR(2)  not null,
    UNIT         INTEGER  not null,
    CURR         INTEGER  not null,
    TOT_YEAR_REC DECIMAL(15),
    TOT_PER_REC  DECIMAL(15),
    constraint IXU_GL_040
        primary key (YEAR, PERIOD, JOURNAL, UNIT, CURR)
);

