create table TMP_FIXING_RATE
(
    ID_CURRENCY     INTEGER        not null,
    RATE            DECIMAL(12, 6) not null,
    ACTIVATION_DATE DATE,
    SHORT_DESCR     CHAR(5),
    DESCRIPTION     VARCHAR(40),
    constraint IXU_REP_126
        primary key (ID_CURRENCY, RATE)
);

