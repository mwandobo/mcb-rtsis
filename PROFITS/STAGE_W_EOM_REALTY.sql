create table STAGE_W_EOM_REALTY
(
    EOM_DATE                     DATE,
    REALTY_KEY                   DECIMAL(10) not null,
    MUNICIPALITY                 VARCHAR(40),
    REAL_ESTATE_DESC             CHAR(40),
    LAST_APPRSAL_EVALUATION_DATE DATE,
    LAND_REGISTRY_NUMBER         VARCHAR(50),
    CITY                         VARCHAR(50),
    REGION                       VARCHAR(60),
    COMMERCIAL_VALUE             DECIMAL(15, 2)
);

