create table DCD_VOUCHER_SETUP
(
    VOUCHER_ID       DECIMAL(12) not null,
    BANK_VOUCHER_KEY INTEGER     not null,
    BANK_VOUCHER_ID  DECIMAL(12),
    constraint IXU_DCD_051
        primary key (BANK_VOUCHER_KEY, VOUCHER_ID)
);

