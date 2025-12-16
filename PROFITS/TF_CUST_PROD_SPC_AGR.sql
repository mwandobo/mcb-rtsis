create table TF_CUST_PROD_SPC_AGR
(
    ID_PRODUCT     INTEGER not null,
    CUST_ID        INTEGER not null,
    COMM_CHRG_FRQ  SMALLINT,
    COMM_DISC      DECIMAL(8, 4),
    COMM_PERC      DECIMAL(8, 4),
    MAX_COMM_AMN   DECIMAL(15, 2),
    MIN_COMM_AMN   DECIMAL(15, 2),
    COMM_AMN       DECIMAL(15, 2),
    EXPIRY_DATE    DATE,
    COMM_CHRG_FRQT CHAR(1),
    constraint IXU_FX_031
        primary key (ID_PRODUCT, CUST_ID)
);

