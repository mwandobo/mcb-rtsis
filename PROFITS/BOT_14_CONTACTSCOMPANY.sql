create table BOT_14_CONTACTSCOMPANY
(
    CONTACTSCOMPANY_ID INTEGER generated always as identity
        constraint BOT_14_CONTACTSCOMPANY_ID_PK
            primary key,
    FK_COMPANY         INTEGER
        constraint BOT_14_FKCOMPANY
            references BOT_10_COMPANY,
    CELLULARPHONE      VARCHAR(16),
    EMAIL              VARCHAR(64),
    FAX                VARCHAR(16),
    FIXEDLINE          VARCHAR(16),
    WEBPAGE            VARCHAR(64),
    REPORTING_DATE     DATE
);

