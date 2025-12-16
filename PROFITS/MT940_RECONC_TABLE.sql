create table MT940_RECONC_TABLE
(
    RECONCIL_SN     DECIMAL(10)  not null,
    BANK_ID         INTEGER      not null,
    MT940_YEAR      SMALLINT     not null,
    STATEMENT_NUM   CHAR(11)     not null,
    MT940_SN        DECIMAL(10)  not null,
    CORR_BANK_SN    DECIMAL(10)  not null,
    RECON_TIMESTAMP TIMESTAMP(6) not null,
    constraint IXU_MT940_003
        primary key (RECONCIL_SN, BANK_ID, MT940_YEAR, MT940_SN, CORR_BANK_SN, STATEMENT_NUM)
);

