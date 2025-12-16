create table CMS_CAF_DTL
(
    FK_CMS_CAF_HDR_SN DECIMAL(10) not null,
    LINE_NO           DECIMAL(10) not null,
    TMSTAMP           TIMESTAMP(6),
    FULL_LINE         VARCHAR(4000),
    CARD_SN           DECIMAL(10),
    ACCOUNTS_COUNT    SMALLINT,
    ACCTS_OCCUP_LGTH  SMALLINT,
    constraint PK_CMS_CAF_HDR
        primary key (FK_CMS_CAF_HDR_SN, LINE_NO)
);

