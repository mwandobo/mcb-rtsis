create table HPROD_PFG_SET
(
    TMSTAMP            TIMESTAMP(6),
    ENTRY_STATUS       CHAR(1),
    PERM_RESTR_IND     CHAR(1),
    FK_HPRODUCTVALIDIT DATE     not null,
    FK_HPRODUCTID_PROD INTEGER  not null,
    FK_PFG_TAG_SET     CHAR(20) not null,
    constraint PK_HPRD_PFG
        primary key (FK_HPRODUCTVALIDIT, FK_HPRODUCTID_PROD, FK_PFG_TAG_SET)
);

