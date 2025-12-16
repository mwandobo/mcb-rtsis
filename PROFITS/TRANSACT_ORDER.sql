create table TRANSACT_ORDER
(
    REFERENCE_NO      INTEGER not null
        constraint IXU_LOA_100
            primary key,
    CUST_ID           INTEGER,
    APPROVAL_DT       DATE,
    APPROVAL_LIMIT    DECIMAL(15, 2),
    APPROVAL_DURATION SMALLINT,
    PRODUCT_ID        INTEGER,
    PROCESSED_FLG     CHAR(1),
    OPEN_DT           DATE
);

