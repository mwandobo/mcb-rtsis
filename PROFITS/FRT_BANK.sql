create table FRT_BANK
(
    BIC_ADDRESS CHAR(11) not null
        constraint PK_FRT_BANK
            primary key,
    BANK_NAME   CHAR(70),
    BANK_CODE   CHAR(3),
    COUNTRY     CHAR(2),
    SORT_CODE   CHAR(11)
);

