create table DCD_VOUCHER_DSP
(
    VOUCHER_ID    DECIMAL(12) not null,
    CUST_LANGUAGE INTEGER     not null,
    VOUCHER_LINE  VARCHAR(2048),
    DESCRIPTION   VARCHAR(40),
    constraint PK_PAR_VOUCHER_DSP
        primary key (VOUCHER_ID, CUST_LANGUAGE)
);

