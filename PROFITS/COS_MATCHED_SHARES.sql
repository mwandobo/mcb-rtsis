create table COS_MATCHED_SHARES
(
    APPLICATION_ID    DECIMAL(11) not null,
    SHARE_ID          DECIMAL(10) not null,
    CREATED_DATE      DATE,
    UPDATED_TIMESTAMP TIMESTAMP(6),
    CREATED_BY        CHAR(8),
    UPDATED_BY        CHAR(8),
    CUSTOMER_ID       INTEGER,
    constraint IXU_CP_073
        primary key (APPLICATION_ID, SHARE_ID)
);

