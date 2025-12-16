create table CMS_CARD_ACCS_HIST
(
    TMSTAMP            TIMESTAMP(6) not null,
    RECORD_SN          DECIMAL(10) generated always as identity,
    ACCOUNT_NUMBER     CHAR(40),
    ACCOUNT_CD         SMALLINT,
    PRFT_SYSTEM        SMALLINT,
    DEFAULT_FLG        CHAR(1),
    CREDIT_CARD_FLG    CHAR(1),
    ENTRY_STATUS       CHAR(1),
    FK_CRD_APPL_SN     DECIMAL(10)  not null
        references CMS_CARD_APPL,
    FK_CARD_SN         DECIMAL(10)  not null
        references CMS_CARD,
    FK_CARD_HISTMSTAMP TIMESTAMP(6)
                                    references CMS_CARD_HIST
                                        on delete set null,
    constraint PK_CARD_ACCS_HIS
        primary key (FK_CRD_APPL_SN, FK_CARD_SN, RECORD_SN, TMSTAMP)
);

create unique index I0000689
    on CMS_CARD_ACCS_HIST (FK_CARD_SN);

create unique index I0000727
    on CMS_CARD_ACCS_HIST (FK_CARD_HISTMSTAMP);

create unique index I0010632
    on CMS_CARD_ACCS_HIST (FK_CRD_APPL_SN);

