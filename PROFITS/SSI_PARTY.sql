create table SSI_PARTY
(
    BIC          CHAR(11) not null
        constraint PK_SSI_PARTY
            primary key,
    BANK_ID      INTEGER  not null,
    OUR_BANK_FLG CHAR(1),
    PREFERENCE   CHAR(2),
    COUNTRY_ISO  CHAR(2),
    TMSTAMP      TIMESTAMP(6)
);

