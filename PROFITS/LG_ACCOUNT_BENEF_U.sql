create table LG_ACCOUNT_BENEF_U
(
    FK_LG_ACCOUNTACC_S DECIMAL(13) not null,
    FK_LG_BENEFICIACOD INTEGER     not null,
    SN                 SMALLINT    not null,
    ENTRY_STATUS       CHAR(1),
    constraint IXU_CIU_045
        primary key (FK_LG_BENEFICIACOD, FK_LG_ACCOUNTACC_S)
);

