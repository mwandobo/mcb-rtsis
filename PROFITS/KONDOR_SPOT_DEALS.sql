create table KONDOR_SPOT_DEALS
(
    SPOT_DATE    DATE     not null,
    SPOT_SRC     SMALLINT not null,
    SPOT_CRDB    CHAR(1)  not null,
    SPOT_AMNT    DECIMAL(15, 2),
    SPOT_AMNT_DC DECIMAL(15, 2),
    constraint PKKNDSPOT
        primary key (SPOT_CRDB, SPOT_SRC, SPOT_DATE)
);

