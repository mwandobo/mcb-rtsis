create table RSKCO_FIXINGS
(
    FIXING_DATE        DATE     not null,
    SECURITY_ID        CHAR(30) not null,
    PRODUCT            CHAR(10) not null,
    FIXING_PRICE_DEC   SMALLINT,
    PRTF_EXTRACTION_DT DATE,
    FIXING_TYPE        CHAR(1),
    FIXING_PRICE       CHAR(10),
    PRFT_ROUTINE       CHAR(20),
    constraint IXU_LNS_045
        primary key (FIXING_DATE, SECURITY_ID, PRODUCT)
);

