create table DSUBDF
(
    SD_ACCESS            INTEGER     not null,
    SD_EXP_OPT           INTEGER     not null,
    FK_DOBJOBJ_ID        DECIMAL(10) not null,
    FK_DSUBIDS_SUBSET_ID DECIMAL(10) not null,
    constraint IDSUBDEF
        primary key (FK_DSUBIDS_SUBSET_ID, FK_DOBJOBJ_ID)
);

create unique index I0000470
    on DSUBDF (FK_DOBJOBJ_ID);

create unique index I0000471
    on DSUBDF (FK_DSUBIDS_SUBSET_ID);

