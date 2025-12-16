create table RPT_TRANSLATIONS
(
    FK_BASE_ID       INTEGER      not null,
    FK_LANGUAGE_ID   INTEGER      not null,
    FK_REP_ID        INTEGER      not null,
    BASE_TRANSLATION VARCHAR(100) not null,
    constraint RPT_TRANSLATIONS_PK
        primary key (FK_BASE_ID, FK_LANGUAGE_ID, FK_REP_ID)
);

