create table SWIFT_ALLNCE_RMA
(
    BIC            CHAR(11) not null,
    SWIFT_TYPE     CHAR(10) not null,
    SWIFT_CONN_IN  CHAR(1)  not null,
    SWIFT_CONN_OUT CHAR(1)  not null,
    START_DATE     DATE,
    END_DATE       DATE,
    constraint PK_SWFT_RMA
        primary key (BIC, SWIFT_TYPE)
);

