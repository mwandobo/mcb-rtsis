create table BOT_22_SUBJECTRELATIONS
(
    SUBJECTRELATIONS_ID   INTEGER generated always as identity
        constraint BOT_22_SUBJECTRELATIONS_ID_PK
            primary key,
    FK_STSUBJECTRELATIONS INTEGER
        constraint BOT_22_FKSTSUBJECTRELATIONS
            references BOT_7_STSUBJECTRELATIONS,
    X__SECONDARYSUBJECT   SMALLINT default 1
);

