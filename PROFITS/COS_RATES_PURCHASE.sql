create table COS_RATES_PURCHASE
(
    ACTIVATION_DATE DATE                  not null,
    PURCHASE_VALUE  DECIMAL(15, 2),
    CREATED_DATE    DATE,
    UPDATED_DATE    DATE,
    CREATED_BY      CHAR(8),
    UPDATED_BY      CHAR(8),
    SERVICE_PRODUCT INTEGER default 38001 not null,
    constraint IXU_CP_076
        primary key (SERVICE_PRODUCT, ACTIVATION_DATE)
);

create unique index IXU_CP_076
    on COS_RATES_PURCHASE (ACTIVATION_DATE);

