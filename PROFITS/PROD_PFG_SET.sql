create table PROD_PFG_SET
(
    TMSTAMP            TIMESTAMP(6),
    ENTRY_STATUS       CHAR(1),
    PERM_RESTR_IND     CHAR(1),
    FK_PFG_TAG_SET     CHAR(20) not null,
    FK_PRODUCTID_PRODU INTEGER  not null,
    constraint PK_PRD_PFG
        primary key (FK_PFG_TAG_SET, FK_PRODUCTID_PRODU)
);

