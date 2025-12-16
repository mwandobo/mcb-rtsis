create table BOT_61_INDCONTACTS
(
    INDCONTACTS_ID INTEGER generated always as identity
        constraint BOT_61_INDCONTACTS_ID_PK
            primary key,
    FK_INDIVIDUAL  INTEGER
        constraint BOT_61_FKINDIVIDUAL
            references BOT_11_INDIVIDUAL,
    CELLULARPHONE  VARCHAR(16),
    EMAIL          VARCHAR(64),
    FAX            VARCHAR(16),
    FIXEDLINE      VARCHAR(16),
    WEBPAGE        VARCHAR(64),
    REPORTING_DATE DATE
);

