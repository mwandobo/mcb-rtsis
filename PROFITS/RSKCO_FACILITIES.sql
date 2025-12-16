create table RSKCO_FACILITIES
(
    REFERENCE_ID       CHAR(30) not null,
    START_DATE         DATE     not null,
    PRODUCT            CHAR(20) not null,
    ISHIGHRISK         SMALLINT,
    AMOUNT_DECIMALS    SMALLINT,
    GLOBAL_LIMIT       DECIMAL(15),
    AMOUNT             DECIMAL(15),
    APPROVAL_DATE      DATE,
    PRFT_EXTRACTION_DT DATE,
    REVISION_DATE      DATE,
    CURRENCY           CHAR(3),
    UNUSED_TYPE        CHAR(10),
    OBLIGOR            CHAR(15),
    PRFT_ROUTINE       CHAR(20),
    SUB_PRODUCT        CHAR(50),
    constraint IXU_LNS_028
        primary key (REFERENCE_ID, START_DATE, PRODUCT)
);

