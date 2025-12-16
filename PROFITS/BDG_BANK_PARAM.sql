create table BDG_BANK_PARAM
(
    BANK_CODE     SMALLINT not null
        constraint BDGBANKP
            primary key,
    CURR_TRX_DATE DATE
);

