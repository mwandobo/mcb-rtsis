create table COS_SHARES
(
    SHARE_ID              DECIMAL(10) not null
        constraint IXU_CP_078
            primary key,
    SHARE_STATUS          SMALLINT,
    MEMBER_ID             DECIMAL(10),
    NOMINAL_PRICE         DECIMAL(15, 2),
    PURCHASE_PRICE        DECIMAL(15, 2),
    ACCOUNTING_PRICE      DECIMAL(15, 2),
    ASSESSMENT_PRICE      DECIMAL(15, 2),
    CREATION_DATE         DATE,
    DELETION_DATE         DATE,
    LAST_ACQUIS_DATE      DATE,
    UPDATED_DATE          DATE,
    CREATED_DATE          DATE,
    UPDATED_TIMESTAMP     TIMESTAMP(6),
    CREATED_BY            CHAR(8),
    UPDATED_BY            CHAR(8),
    CAPITAL_INCREASE_FLAG CHAR(1),
    SERVICE_PRODUCT       INTEGER
);

create unique index IXN_CP_999
    on COS_SHARES (MEMBER_ID);

