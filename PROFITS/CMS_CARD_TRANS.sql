create table CMS_CARD_TRANS
(
    TRANS_SN             DECIMAL(10) generated always as identity,
    DESCRIPTION          CHAR(80)    not null,
    TMSTAMP              TIMESTAMP(6),
    ENTRY_STATUS         CHAR(1),
    FK_CRD_APPL_SN       DECIMAL(10) not null,
    FK_CARD_SN           DECIMAL(10) not null,
    FK_TRNTYP_GENERIC_HD CHAR(5),
    FK_TRNTYP_GENERIC_SN INTEGER,
    constraint PK_CMS_TRANS_TYPE
        primary key (TRANS_SN, FK_CRD_APPL_SN, FK_CARD_SN)
);

create unique index I0001063
    on CMS_CARD_TRANS (FK_CARD_SN);

create unique index I0001144
    on CMS_CARD_TRANS (FK_TRNTYP_GENERIC_HD, FK_TRNTYP_GENERIC_SN);

create unique index I0011021
    on CMS_CARD_TRANS (FK_CRD_APPL_SN);

