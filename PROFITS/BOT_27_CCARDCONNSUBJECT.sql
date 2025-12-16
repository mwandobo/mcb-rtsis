create table BOT_27_CCARDCONNSUBJECT
(
    CCARDCONNSUBJECT_ID        INTEGER generated always as identity
        constraint BOT_27_CCARDCONNSUBJECT_ID_PK
            primary key,
    FK_CREDITCARD              INTEGER
        constraint BOT_27_FKCREDITCARD
            references BOT_8_CREDITCARD,
    KEY                        VARCHAR(32) not null,
    FK_BOT_30_CONNECTEDSUBJECT INTEGER
        constraint FK_BOT_27_BOT_30__
            references BOT_30_CONNECTEDSUBJECT
);

