create table ATTR_VIEW
(
    ID               DECIMAL(10) not null
        constraint ID0
            primary key,
    SEQ              DECIMAL(10) not null,
    FK_ATTRIBUTEID   DECIMAL(10),
    FK_ENTITY_VIEWID DECIMAL(10)
);

create unique index I0000524
    on ATTR_VIEW (FK_ATTRIBUTEID);

create unique index I0000533
    on ATTR_VIEW (FK_ENTITY_VIEWID);

