create table IPS_SYNC_GLOBAL_ORDERS
(
    SN                     INTEGER  not null
        constraint PKEY0
            primary key,
    INSTRUM_CODE           CHAR(10) not null,
    DIRECTION              CHAR(1)  not null,
    ORDER_STATUS           CHAR(1)  not null,
    REJECTION_CODE         VARCHAR(40),
    PROCESS_RESULTS        VARCHAR(255),
    SYNC_RELATED_ORDERS    CHAR(1),
    COMMAND                VARCHAR(80),
    GLB_PROCESSING_STATUS  SMALLINT,
    GLB_PROCESSED_STATUS   SMALLINT,
    GLB_PROCESSING_RESULTS VARCHAR(250),
    DESCRIPTION            VARCHAR(255),
    GLB_IPROFITS_STATUS    SMALLINT
);

