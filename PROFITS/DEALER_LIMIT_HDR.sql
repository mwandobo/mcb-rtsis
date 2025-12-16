create table DEALER_LIMIT_HDR
(
    DEALER_CODE     CHAR(8) not null
        constraint PK_DEALERCODE_HDR
            primary key,
    TOTAL_LIMIT     DECIMAL(15, 2),
    EXPIRATION_DATE DATE,
    ENTRY_STATUS    CHAR(1),
    TMSTAMP         TIMESTAMP(6)
);

