create table DFM_SERVER_SETUP
(
    ID              INTEGER not null
        constraint PKDFMSER
            primary key,
    BANK_PARAMS_IND CHAR(1),
    CLOSED_UNIT_IND CHAR(1),
    TARGET_JOURNAL  CHAR(1),
    SOURCE_JOURNAL  CHAR(1),
    USER_TOTALS_IND CHAR(1),
    VOUCHER_IND     CHAR(1),
    SWIFT_IND       CHAR(1),
    DIAS_IND        CHAR(1),
    HELMES_IND      CHAR(1) not null,
    TMSTAMP         TIMESTAMP(6)
);

