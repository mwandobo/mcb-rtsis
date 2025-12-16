create table RSKCO_LCG
(
    REFERENCE_ID       CHAR(50) not null
        constraint IXU_LNS_029
            primary key,
    IS_REG_ELIGIBLE    SMALLINT,
    IS_EXPOSURE        SMALLINT,
    IS_LG              SMALLINT,
    IS_GROUP           SMALLINT,
    AMOUNT_DECIMALS    SMALLINT,
    COVERINGPERC       DECIMAL(10),
    AMOUNT             DECIMAL(15),
    PRFT_EXTRACTION_DT DATE,
    EXPIRY_DATE        DATE,
    APPLICATION_DATE   DATE,
    ISSUING_DATE       DATE,
    CURRENCY           CHAR(3),
    LCG_TYPE           CHAR(10),
    MANAGED_ACCOUNT    CHAR(15),
    APPLICANT          CHAR(15),
    PRFT_ROUTINE       CHAR(20),
    ACCOUNTINGNUMBER   CHAR(25),
    ISSUER             CHAR(30),
    FACILITY_REFERENCE CHAR(50)
);

