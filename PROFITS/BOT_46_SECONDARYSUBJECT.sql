create table BOT_46_SECONDARYSUBJECT
(
    SECONDARYSUBJECT_ID     INTEGER generated always as identity
        constraint BOT_46_SECONDARYSUBJECT_ID_PK
            primary key,
    ADDITIONALINFORMATION   VARCHAR(256),
    PERCENTAGEOFSHARESOWNED DECIMAL(19, 4),
    RELATIONTYPE            INTEGER not null,
    FK_BOT_2_SUBJECTCHOICE  INTEGER
        constraint FK_BOT_46_BOT_2__
            references BOT_2_SUBJECTCHOICE
);

