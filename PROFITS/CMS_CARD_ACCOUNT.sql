create table CMS_CARD_ACCOUNT
(
    RECORD_SN       DECIMAL(10) generated always as identity
        constraint PK_CARD_ACCOUNT
            primary key,
    ACCOUNT_NUMBER  CHAR(40),
    ACCOUNT_CD      SMALLINT,
    PRFT_SYSTEM     SMALLINT not null,
    DEFAULT_FLG     CHAR(1),
    CREDIT_CARD_FLG CHAR(1),
    TMSTAMP         TIMESTAMP(6),
    ENTRY_STATUS    CHAR(1),
    FK_CARD_SN      DECIMAL(10),
    FK_CRD_APPL_SN  DECIMAL(10)
);

create unique index ATM_ONL_TRAN_INDX1
    on CMS_CARD_ACCOUNT (ENTRY_STATUS, ACCOUNT_NUMBER, ACCOUNT_CD, PRFT_SYSTEM, FK_CARD_SN, RECORD_SN);

create unique index I0001122
    on CMS_CARD_ACCOUNT (FK_CARD_SN);

create unique index I0001138
    on CMS_CARD_ACCOUNT (FK_CRD_APPL_SN);

