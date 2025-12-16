create table DSUBEX
(
    SE_CKO_STATUS   CHAR(1)     not null,
    FK_DCKOIDCKO_ID DECIMAL(10) not null,
    FK_DOBJOBJ_ID   DECIMAL(10) not null,
    FK0DCKOIDCKO_ID DECIMAL(10),
    FK_DMDLMODEL_ID DECIMAL(10),
    constraint IDSUBEXP
        primary key (FK_DOBJOBJ_ID, FK_DCKOIDCKO_ID)
);

create unique index I0000608
    on DSUBEX (FK0DCKOIDCKO_ID);

