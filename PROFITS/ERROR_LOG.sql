create table ERROR_LOG
(
    TIMESTAMP       TIMESTAMP(6) not null
        constraint IXU_CP_091
            primary key,
    PROFITS_ACC_CD  SMALLINT,
    CP_AGREEMENT_NO DECIMAL(10),
    TRX_AMOUNT      DECIMAL(15, 2),
    TRX_DATE        DATE,
    PROGRAM_ID      CHAR(5),
    PROFITS_ACC_NO  CHAR(40),
    ERROR_DESC      CHAR(250)
);

