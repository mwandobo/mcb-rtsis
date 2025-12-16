create table BENCHMARK_IBAN
(
    SEND_BIC_ADDRESS  CHAR(40) not null,
    SEND_IBAN_ACCOUNT CHAR(40) not null,
    SEND_AMOUNT       DECIMAL(15, 2),
    SEND_BENEF_NAME   CHAR(100),
    BENCHMARK_USED    CHAR(1),
    constraint PK_BNCH_100
        primary key (SEND_IBAN_ACCOUNT, SEND_BIC_ADDRESS)
);

