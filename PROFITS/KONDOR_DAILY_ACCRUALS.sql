create table KONDOR_DAILY_ACCRUALS
(
    ACCR_DATE DATE     not null,
    ACCR_SRC  SMALLINT not null,
    ACCR_CRDB CHAR(1)  not null,
    ACCR_AMNT DECIMAL(15, 2),
    constraint PK_KNDACCR
        primary key (ACCR_CRDB, ACCR_SRC, ACCR_DATE)
);

