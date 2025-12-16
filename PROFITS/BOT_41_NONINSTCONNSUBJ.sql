create table BOT_41_NONINSTCONNSUBJ
(
    NONINSTCONNSUBJ_ID         INTEGER generated always as identity
        constraint BOT_41_NONINSTCONNSUBJ_ID_PK
            primary key,
    FK_NONINSTALMENT           INTEGER
        constraint BOT_41_FKNONINSTALMENT
            references BOT_21_NONINSTALMENT,
    KEY                        VARCHAR(32) not null,
    FK_BOT_30_CONNECTEDSUBJECT INTEGER
        constraint FK_BOT_41_BOT_30__
            references BOT_30_CONNECTEDSUBJECT
);

