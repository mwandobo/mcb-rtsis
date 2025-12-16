create table PROD_DT_TOT_EXP
(
    ID_PRODUCT     INTEGER        not null,
    VALUE_DT       DATE           not null,
    SCALE_AMN      DECIMAL(15, 2) not null,
    FIRST_CHNG_INT DECIMAL(15, 2),
    SEC_CHNG_INT   DECIMAL(15, 2),
    YEAR_CHNG      DECIMAL(15, 2),
    TOT_EXP_AMN    DECIMAL(15, 2),
    constraint IXU_PRD_021
        primary key (ID_PRODUCT, VALUE_DT, SCALE_AMN)
);

