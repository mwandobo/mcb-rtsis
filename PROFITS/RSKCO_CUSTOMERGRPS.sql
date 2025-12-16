create table RSKCO_CUSTOMERGRPS
(
    CUSTOMER_REF       CHAR(50) not null,
    GROUP_REF          CHAR(30) not null,
    PERCENT_DECIMALS   SMALLINT,
    PERCENT            SMALLINT,
    PRFT_EXTRACTION_DT DATE,
    PRFT_ROUTINE       CHAR(20),
    constraint IXU_LNS_027
        primary key (CUSTOMER_REF, GROUP_REF)
);

