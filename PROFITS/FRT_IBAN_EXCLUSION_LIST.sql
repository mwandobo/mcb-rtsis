create table FRT_IBAN_EXCLUSION_LIST
(
    MODIFICATION_FLAG CHAR(1)      not null,
    RECORD_KEY        CHAR(12)     not null
        constraint PK_RECORD_KEY0
            primary key,
    CN                CHAR(2)      not null,
    IBAN_NAT_ID       VARCHAR(15)  not null,
    BIC               CHAR(11),
    VALID_FROM        CHAR(8),
    TMSTAMP           TIMESTAMP(6) not null
);

comment on table FRT_IBAN_EXCLUSION_LIST is 'DATA ABOUT INVALID COMBINATIONS OF INVALID NAT IDS IN IBANS';

create unique index IX_CN_NATID0
    on FRT_IBAN_EXCLUSION_LIST (IBAN_NAT_ID, CN);

