create table TF_SPC_AGR
(
    ID_JUSTIFIC    INTEGER  not null,
    ID_TRANSACT    INTEGER  not null,
    TF_LC_ACC      CHAR(40) not null,
    COMM_CHRG_FRQ  SMALLINT,
    COMM_DISC      DECIMAL(8, 4),
    COMM_PERC      DECIMAL(8, 4),
    MAX_COMM_AMN   DECIMAL(15, 2),
    COMM_AMN       DECIMAL(15, 2),
    MIN_COMM_AMN   DECIMAL(15, 2),
    EXPIRY_DATE    DATE,
    TF_LC_FLAG     CHAR(1),
    COMM_CHRG_FRQT CHAR(1),
    constraint IXU_FX_048
        primary key (ID_JUSTIFIC, ID_TRANSACT, TF_LC_ACC)
);

