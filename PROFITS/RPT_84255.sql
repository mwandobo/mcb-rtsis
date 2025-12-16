create table RPT_84255
(
    DESCRIPTION_RISK  CHAR(40),
    DESCRIPTION_BENEF CHAR(40),
    WITHOUT_COL       DECIMAL(15, 2),
    REST_OF_COL       DECIMAL(15, 2),
    REAL_ESTATE_COL   DECIMAL(15, 2),
    BANK_GUARANTEE    DECIMAL(15, 2),
    CASH              DECIMAL(15, 2),
    TOTAL_AMN         DECIMAL(15, 2),
    CLNS_TYPE         INTEGER not null,
    CRISK             INTEGER not null,
    constraint IXU_REP_024
        primary key (CRISK, CLNS_TYPE)
);

