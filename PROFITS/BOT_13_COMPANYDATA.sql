create table BOT_13_COMPANYDATA
(
    COMPANYDATA_ID          INTEGER generated always as identity
        constraint BOT_13_COMPANYDATA_ID_PK
            primary key,
    FK_COMPANY              INTEGER
        constraint BOT_13_FKCOMPANY
            references BOT_10_COMPANY,
    ESTABLISHMENTDATE       DATE,
    LEGALFORM               INTEGER,
    NEGATIVESTATUSOFCLIENT  INTEGER,
    NUMBEROFEMPLOYEES       INTEGER,
    REGISTRATIONCOUNTRY     INTEGER,
    REGISTRATIONNUMBER      VARCHAR(32),
    TAXIDENTIFICATIONNUMBER VARCHAR(16),
    TRADENAME               VARCHAR(128),
    REPORTING_DATE          DATE
);

