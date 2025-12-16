create table STAT_SCALE
(
    UP_LIMIT   DECIMAL(15, 4),
    LOW_LIMIT  DECIMAL(15, 4),
    ID_PROGRAM CHAR(5)
);

create unique index IXP_STA_006
    on STAT_SCALE (UP_LIMIT, LOW_LIMIT, ID_PROGRAM);

