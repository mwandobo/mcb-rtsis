create table REP_74672
(
    PROGRAM_ID  INTEGER  not null,
    CATEGORY_SN SMALLINT not null,
    MID_INT     DECIMAL(9, 6),
    TOT_ACC     DECIMAL(15, 2),
    MID_TOT_ACC DECIMAL(15, 2),
    constraint IXU_REP_210
        primary key (PROGRAM_ID, CATEGORY_SN)
);

