create table DCKOID
(
    CKO_MODEL_ID         DECIMAL(10) not null,
    CKO_ID               DECIMAL(10) not null
        constraint IDCKO
            primary key,
    CKO_USER_ID          CHAR(8)     not null,
    CKO_DATE             DATE        not null,
    CKO_TIME             TIME        not null,
    CKO_STATUS           CHAR(1)     not null,
    CKO_COMMIT_CNT       DECIMAL(10),
    FK_DSUBIDS_SUBSET_ID DECIMAL(10)
);

create unique index I0000609
    on DCKOID (FK_DSUBIDS_SUBSET_ID);

