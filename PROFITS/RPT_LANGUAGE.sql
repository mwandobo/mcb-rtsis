create table RPT_LANGUAGE
(
    ID          INTEGER     not null,
    NAME        VARCHAR(50) not null,
    DESCRIPTION VARCHAR(200)
);

create unique index RPT_LANGUAGE_PK
    on RPT_LANGUAGE (ID);

alter table RPT_LANGUAGE
    add constraint RPT_LANGUAGEO_PK
        primary key (ID);

