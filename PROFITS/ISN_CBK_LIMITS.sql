create table ISN_CBK_LIMITS
(
    REPORT_DATE DATE           not null,
    LIMCAT      VARCHAR(10)    not null,
    LIMITGRP    VARCHAR(10)    not null,
    LIMPERIOD   VARCHAR(10)    not null,
    LIMIT_AMT   DECIMAL(18, 4) not null,
    WORK_AMT    DECIMAL(18, 4) not null,
    USED_AMT    DECIMAL(18, 4) not null,
    AVAIL_AMT   DECIMAL(18, 4) not null,
    RECDESC     VARCHAR(35),
    constraint PK_ISN_CBK_LIMITS
        primary key (REPORT_DATE, LIMCAT, LIMITGRP, LIMPERIOD)
);

