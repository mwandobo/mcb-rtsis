create table CMS_CARD_TRANS_HIS
(
    TMSTAMP              TIMESTAMP(6) not null,
    TRANS_SN             DECIMAL(10) generated always as identity,
    DESCRIPTION          CHAR(80),
    ENTRY_STATUS         CHAR(1),
    FK_TRNTYP_GENERIC_HD CHAR(5),
    FK_TRNTYP_GENERIC_SN INTEGER,
    constraint PK_TRANS_TYPE_HIS
        primary key (TRANS_SN, TMSTAMP)
);

create unique index I0001075
    on CMS_CARD_TRANS_HIS (FK_TRNTYP_GENERIC_HD, FK_TRNTYP_GENERIC_SN);

