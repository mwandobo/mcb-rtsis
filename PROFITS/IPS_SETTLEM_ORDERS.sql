create table IPS_SETTLEM_ORDERS
(
    ORDER_FILENAME   VARCHAR(50) not null,
    LINE_NO          DECIMAL(10) not null,
    ORDERS_ORIGIN    CHAR(5),
    BANK_NO          CHAR(2),
    CURRENCY_CODE    CHAR(2),
    ORDER_AMOUNT     DECIMAL(15, 2),
    JUSTIFIC_ID      INTEGER,
    COMPLETE_MESSAGE VARCHAR(2048),
    TRX_DATE         DATE,
    TMSTAMP          TIMESTAMP(6),
    GROUP_ID         DECIMAL(10) not null,
    SETTL_PROCESSED  CHAR(1),
    constraint IPS_SETTLEM_ORDERS_PK
        primary key (ORDER_FILENAME, GROUP_ID, LINE_NO)
);

