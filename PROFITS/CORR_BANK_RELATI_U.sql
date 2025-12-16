create table CORR_BANK_RELATI_U
(
    FK_CURRENCYID_CURR INTEGER      not null,
    FK_OTHER_BANKFK_GE CHAR(5)      not null,
    FK_OTHER_BANKFK_G1 INTEGER      not null,
    FK_OTHER_BANKDISCR CHAR(15)     not null,
    FK_CUSTOMERCUST_ID INTEGER      not null,
    TMSTAMP            TIMESTAMP(6) not null,
    constraint IXU_CIU_011
        primary key (FK_CUSTOMERCUST_ID, FK_OTHER_BANKDISCR, FK_OTHER_BANKFK_G1, FK_OTHER_BANKFK_GE, FK_CURRENCYID_CURR)
);

