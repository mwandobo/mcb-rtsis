create table BOT_34_PERSONSPOUSE
(
    PERSONSPOUSE_ID          INTEGER generated always as identity
        constraint BOT_34_PERSONSPOUSE_ID_PK
            primary key,
    FK_PERSONALDATA          INTEGER
        constraint BOT_34_FKPERSONALDATA
            references BOT_63_PERSONALDATA,
    KEY                      VARCHAR(32) not null,
    FK_BOT_45_SPOUSEFULLNAME INTEGER,
    REPORTING_DATE           DATE
);

