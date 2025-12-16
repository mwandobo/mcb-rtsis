create table COS_RATES_NOM_ACC
(
    ACTIVATION_DATE DATE                  not null,
    RATE_TYPE       SMALLINT              not null,
    RATE_VALUE      DECIMAL(15, 2),
    DECISION_DATE   DATE,
    UPDATED_DATE    DATE,
    CREATED_DATE    DATE,
    CREATED_BY      CHAR(8),
    UPDATED_BY      CHAR(8),
    COMMENTS        VARCHAR(255),
    SERVICE_PRODUCT INTEGER default 38001 not null,
    constraint IXU_CP_075
        primary key (SERVICE_PRODUCT, ACTIVATION_DATE, RATE_TYPE)
);

create unique index IXU_CP_075
    on COS_RATES_NOM_ACC (ACTIVATION_DATE, RATE_TYPE);

