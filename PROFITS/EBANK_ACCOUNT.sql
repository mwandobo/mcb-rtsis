create table EBANK_ACCOUNT
(
    ACCOUNT_NUMBER DECIMAL(10) not null
);

create unique index PKEBANK
    on EBANK_ACCOUNT (ACCOUNT_NUMBER);

