create table BOT_25_SECONDARYSUBJECT
(
    SECONDARYSUBJECT_ID        INTEGER generated always as identity
        constraint BOT_25_SECONDARYSUBJECT_ID_PK
            primary key,
    FK_SUBJECTRELATIONS        INTEGER
        constraint BOT_25_FKSUBJECTRELATIONS
            references BOT_22_SUBJECTRELATIONS,
    KEY                        VARCHAR(32) not null,
    FK_BOT_46_SECONDARYSUBJECT INTEGER
        constraint FK_BOT_25_BOT_46__
            references BOT_46_SECONDARYSUBJECT
);

