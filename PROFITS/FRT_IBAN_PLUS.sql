create table FRT_IBAN_PLUS
(
    MODIFICATION_FLAG CHAR(1)      not null,
    RECORD_KEY        CHAR(12)     not null
        constraint PK_RECORD_KEY
            primary key,
    INSTITUTION_NAME  VARCHAR(105) not null,
    COUNTRY_NAME      VARCHAR(70)  not null,
    ISO_CN            CHAR(2)      not null,
    ISO_IBAN_CN       CHAR(2)      not null,
    IBAN_BIC          CHAR(11)     not null,
    ROUTING_BIC       VARCHAR(11)  not null,
    IBAN_NAT_ID       VARCHAR(15)  not null,
    SERVICE_CONTEXT   VARCHAR(8),
    FIELD_A           VARCHAR(35),
    FIELD_B           VARCHAR(35),
    TMSTAMP           TIMESTAMP(6) not null
);

comment on table FRT_IBAN_PLUS is 'DATA ABOUT SEPA IBAN & BICS';

create unique index IX_CN_NATID
    on FRT_IBAN_PLUS (IBAN_NAT_ID, ISO_IBAN_CN);

