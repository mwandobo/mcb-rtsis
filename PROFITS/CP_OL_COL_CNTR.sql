create table CP_OL_COL_CNTR
(
    CP_OL_COUNTER DECIMAL(15) generated always as identity
        constraint PK_CP_OL_CNTR
            primary key,
    TMSTAMP       TIMESTAMP(6)
);

