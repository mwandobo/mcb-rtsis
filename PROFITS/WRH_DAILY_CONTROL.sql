create table WRH_DAILY_CONTROL
(
    DATE_ID           DATE not null
        constraint IXU_EOM_014
            primary key,
    LOANS_EXEC_FLG    SMALLINT,
    DEPOSITS_EXEC_FLG SMALLINT,
    MM_EXEC_FLG       SMALLINT,
    LOANS_EXEC_DT     DATE,
    MM_EXEC_DT        DATE,
    DEPOSITS_EXEC_DT  DATE,
    HOLIDAY_IND       CHAR(1),
    DAY_NAME          CHAR(3),
    LOANS_EXEC_ERR    VARCHAR(300),
    DEPOSITS_EXEC_ERR VARCHAR(300)
);

