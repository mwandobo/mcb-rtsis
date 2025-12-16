create table GLOBAL_LINKED_ORDERS
(
    SERIAL_NO                     SMALLINT    not null,
    ORIGINATING_ORDER_ID          DECIMAL(15) not null,
    PREVIOUS_ORDER_ID             DECIMAL(15),
    EXECUTED_ORDER_ID             DECIMAL(15) not null,
    ORIGINATING_INTERNAL_ORDER_ID VARCHAR(50) not null,
    PREVIOUS_INTERNAL_ORDER_ID    VARCHAR(50),
    EXECUTED_INTERNAL_ORDER_ID    VARCHAR(50) not null,
    TRX_DATE                      DATE,
    TRX_USR                       VARCHAR(8),
    TRX_TIMESTAMP                 TIMESTAMP(6),
    DIRECTION                     CHAR(1)     not null,
    MESSAGE_TYPE                  VARCHAR(40) not null,
    ORDER_AMOUNT                  DECIMAL(18, 2),
    ORDER_CURRENCY                INTEGER,
    SOURCE_ACCOUNT_NUMBER         CHAR(40),
    DESTINATION_ACCOUNT           VARCHAR(37),
    ID_PRODUCT                    INTEGER,
    ID_TRANSACT                   INTEGER,
    ID_JUSTIFIC                   INTEGER,
    PROCESS_TYPE                  VARCHAR(10),
    COMMENTS                      VARCHAR(250),
    constraint PK_GLOBAL_LO
        primary key (EXECUTED_ORDER_ID, EXECUTED_INTERNAL_ORDER_ID)
);

create unique index IX_GLB_LO
    on GLOBAL_LINKED_ORDERS (ORIGINATING_ORDER_ID);

