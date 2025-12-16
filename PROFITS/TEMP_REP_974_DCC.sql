create table TEMP_REP_974_DCC
(
    ACCOUNT_ID      VARCHAR(22) not null
        constraint IXU_REP_187
            primary key,
    SORTING         VARCHAR(3),
    CHECK_BALLANCE  VARCHAR(27),
    BALLANCE        VARCHAR(27),
    DESCRIPTION     VARCHAR(60),
    SUM_DISTRIBUTED VARCHAR(4000),
    DISTRIBUTION    VARCHAR(4000)
);

