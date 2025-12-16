create table LMS_DUAL
(
    TTC             CHAR(250) not null
        constraint PK_LMSDUAL
            primary key,
    CHECK_1_TMSTAMP TIMESTAMP(6),
    CHECK_2_DECIMAL DECIMAL(15, 2),
    CHECK_3_NUMBER  INTEGER,
    CHECK_5_DATE    DATE,
    CHECK_4_TIME    TIME
);

