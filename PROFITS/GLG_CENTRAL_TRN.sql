create table GLG_CENTRAL_TRN
(
    SUBSYSTEM          SMALLINT       not null,
    UNIT_CODE          INTEGER        not null,
    TRX_DATE           DATE           not null,
    CURRENCY           INTEGER        not null,
    CENTRAL_ACCOUNT    CHAR(21)       not null,
    CENTRAL_FLAG       CHAR(1)        not null,
    DB_CENTRAL_AMNT    DECIMAL(15, 2) not null,
    CR_CENTRAL_AMNT    DECIMAL(15, 2) not null,
    CENTRALIZED_DB_AMN DECIMAL(15, 2) not null,
    CENTRALIZED_CR_AMN DECIMAL(15, 2) not null,
    constraint PIXGLGCE
        primary key (CENTRAL_ACCOUNT, CURRENCY, TRX_DATE, SUBSYSTEM, UNIT_CODE)
);

