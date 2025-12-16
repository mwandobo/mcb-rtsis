create table PAR_VOUCHER_DSP
(
    VOUCHER_ID   INTEGER not null,
    CUST_LANG    INTEGER not null,
    DESCRIPTION  VARCHAR(40),
    VOUCHER_LINE VARCHAR(2048),
    constraint IXU_PRD_029
        primary key (VOUCHER_ID, CUST_LANG)
);

