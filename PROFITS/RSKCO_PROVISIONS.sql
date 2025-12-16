create table RSKCO_PROVISIONS
(
    REFERENCE_ID       CHAR(50) not null
        constraint IXU_LNS_050
            primary key,
    INCREASEDECREASE   SMALLINT,
    AMOUNT             SMALLINT,
    PROV_DATE          DATE,
    PRFT_EXTRACTION_DT DATE,
    PRFT_ROUTINE       CHAR(20),
    PRODUCTTYPE        CHAR(50),
    EXPOSUREREF        CHAR(50)
);

