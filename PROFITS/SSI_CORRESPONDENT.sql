create table SSI_CORRESPONDENT
(
    CORRESPONDENT_BIC  CHAR(11) not null,
    TRADE_TYPE         CHAR(5)  not null,
    FIN_COPY_SERVICE   CHAR(5)  not null,
    PARTY_IDENTIFIER   CHAR(60),
    FK_SSI_PARTY_BIC   CHAR(11) not null,
    FK_CORR_CURRENCY   INTEGER  not null,
    PREFERENCE         CHAR(2),
    COUNTRY_ISO        CHAR(2),
    CUST_ID            INTEGER,
    BANK_ID            INTEGER,
    FK_PROFITS_ACCOUNT CHAR(40),
    OUR_CORR_FLG       CHAR(1),
    TMSTAMP            TIMESTAMP(6),
    constraint PK_SSI_CORRESP
        primary key (FK_SSI_PARTY_BIC, CORRESPONDENT_BIC, FK_CORR_CURRENCY, FIN_COPY_SERVICE, TRADE_TYPE)
);

create unique index I0000562
    on SSI_CORRESPONDENT (FK_SSI_PARTY_BIC);

create unique index I0000563
    on SSI_CORRESPONDENT (FK_CORR_CURRENCY);

create unique index I0000571
    on SSI_CORRESPONDENT (FK_PROFITS_ACCOUNT);

