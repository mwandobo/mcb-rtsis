create table COS_RATES_NEGOTIATION
(
    ACTIVATION_DATE DATE                  not null,
    MAXIMUM_VALUE   DECIMAL(15, 2),
    MINIMUM_VALUE   DECIMAL(15, 2),
    CREATED_DATE    DATE,
    UPDATED_DATE    DATE,
    CREATED_BY      CHAR(8),
    UPDATED_BY      CHAR(8),
    SERVICE_PRODUCT INTEGER default 38001 not null,
    constraint IXU_CP_111
        primary key (SERVICE_PRODUCT, ACTIVATION_DATE)
);

create unique index IXU_CP_111
    on COS_RATES_NEGOTIATION (ACTIVATION_DATE);

