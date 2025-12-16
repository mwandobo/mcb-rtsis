create table RSKCO_CFTRANSACT
(
    REFERENCE_ID       CHAR(30) not null
        constraint IXU_LNS_026
            primary key,
    AMOUNT_DECIMALS    SMALLINT,
    AMOUNT             DECIMAL(15),
    PRFT_EXTRACTION_DT DATE,
    PAYMENT_DATE       DATE,
    TRANSACTION_DATE   DATE,
    DW                 CHAR(1),
    CURRENCY           CHAR(3),
    TRANSACTION_TYPE   CHAR(10),
    OBLIGOR            CHAR(15),
    PRFT_ROUTINE       CHAR(20),
    ACOUNT_NUMBER      CHAR(20),
    BANK_NAME          CHAR(50)
);

