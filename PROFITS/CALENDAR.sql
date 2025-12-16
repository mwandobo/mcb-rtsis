create table CALENDAR
(
    DAY_ID          SMALLINT,
    WEEK_NUM        SMALLINT,
    YR_WORK_DAY_NUM SMALLINT,
    YR_DAY_NUM      SMALLINT,
    DATE_ID         DATE,
    HOLIDAY_IND     CHAR(1),
    DAY_NAME        CHAR(3)
);

create unique index IXN_CAL_100
    on CALENDAR (DATE_ID);

create unique index IXN_CAL_101
    on CALENDAR (YR_WORK_DAY_NUM, HOLIDAY_IND);

create unique index IXU_CAL_000
    on CALENDAR (DAY_ID);

