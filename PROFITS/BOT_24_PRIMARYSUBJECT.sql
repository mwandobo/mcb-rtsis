create table BOT_24_PRIMARYSUBJECT
(
    PRIMARYSUBJECT_ID      INTEGER generated always as identity
        constraint BOT_24_PRIMARYSUBJECT_ID_PK
            primary key,
    FK_SUBJECTRELATIONS    INTEGER
        constraint BOT_24_FKSUBJECTRELATIONS
            references BOT_22_SUBJECTRELATIONS,
    X__SUBJECTCHOICE       SMALLINT default 1,
    FK_BOT_2_SUBJECTCHOICE INTEGER
        constraint FK_BOT_24_BOT_2__
            references BOT_2_SUBJECTCHOICE
);

