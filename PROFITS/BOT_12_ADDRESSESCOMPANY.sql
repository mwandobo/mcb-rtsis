create table BOT_12_ADDRESSESCOMPANY
(
    ADDRESSESCOMPANY_ID INTEGER generated always as identity
        constraint BOT_12_ADDRESSESCOMPANY_ID_PK
            primary key,
    FK_COMPANY          INTEGER
        constraint BOT_12_FKCOMPANY
            references BOT_10_COMPANY,
    X__BUSINESS         SMALLINT default 1,
    X__POSTAL           SMALLINT default 1,
    X__REGISTRATION     SMALLINT default 1,
    REPORTING_DATE      DATE
);

