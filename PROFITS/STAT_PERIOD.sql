create table STAT_PERIOD
(
    CURRENT_YEAR SMALLINT,
    DATE_FROM    DATE,
    DATE_TO      DATE
);

create unique index IXP_STA_010
    on STAT_PERIOD (CURRENT_YEAR);

