create table RSKCO_CUSTOMER
(
    CUSTOMER_ID        CHAR(30) not null
        constraint IXU_LNS_042
            primary key,
    ISSME              SMALLINT,
    PRFT_EXTRACTION_DT DATE,
    OPENING_DATE       DATE,
    COUNTRY            CHAR(10),
    TAX_ID             CHAR(20),
    PRFT_ROUTINE       CHAR(20),
    TAX_AUTHORITY      CHAR(20),
    JOB                CHAR(30),
    OBLIGOR_TYPE       CHAR(50),
    PROFITCENTERCODE   CHAR(50),
    FAX_NUMBER_JOB     CHAR(50),
    PHONE_NUMBER_JOB   CHAR(50),
    FAX_NUMBER         CHAR(50),
    TELEPHONE_NUMBER   CHAR(50),
    ZIP_CODE           CHAR(50),
    CITY               CHAR(50),
    ROAD_NAME          CHAR(50),
    FULL_NAME          CHAR(50)
);

