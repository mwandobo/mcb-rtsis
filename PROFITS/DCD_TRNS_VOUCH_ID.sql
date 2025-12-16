create table DCD_TRNS_VOUCH_ID
(
    CODE          CHAR(8)      not null,
    CUST_LANGUAGE INTEGER      not null,
    TMPSTAMP      TIMESTAMP(6) not null,
    VOUCHER_ID    DECIMAL(12)  not null,
    STATUS        CHAR(1),
    PASSWORD      CHAR(26),
    VOUCHER_DESC  CHAR(80),
    constraint IXU_DEF_081
        primary key (CODE, CUST_LANGUAGE, TMPSTAMP, VOUCHER_ID)
);

