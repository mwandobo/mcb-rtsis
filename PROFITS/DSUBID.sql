create table DSUBID
(
    S_MODEL_ID      DECIMAL(10) not null,
    S_SUBSET_ID     DECIMAL(10) not null
        constraint IDSUBS
            primary key,
    S_SUBSET_NAME   VARCHAR(32) not null,
    S_OWNER_ID      CHAR(8)     not null,
    S_SUBSET_TYPE   CHAR(1)     not null,
    FK_DCKOIDCKO_ID DECIMAL(10),
    FK_DMDLMODEL_ID DECIMAL(10)
);

create unique index I0000612
    on DSUBID (FK_DMDLMODEL_ID);

create unique index I0000615
    on DSUBID (FK_DCKOIDCKO_ID);

