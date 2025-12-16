create table DCD_VOUCHER_ID
(
    VOUCHER_ID    DECIMAL(12) not null,
    CUST_LANGUAGE INTEGER     not null,
    VOUCHER_DESC  CHAR(80),
    constraint PKDCDVC1
        primary key (VOUCHER_ID, CUST_LANGUAGE)
);

