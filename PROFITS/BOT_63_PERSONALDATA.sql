create table BOT_63_PERSONALDATA
(
    PERSONALDATA_ID          INTEGER generated always as identity
        constraint BOT_63_PERSONALDATA_ID_PK
            primary key,
    FK_INDIVIDUAL            INTEGER
        constraint BOT_63_FKINDIVIDUAL
            references BOT_11_INDIVIDUAL,
    BIRTHSURNAME             VARCHAR(64),
    CITIZENSHIP              INTEGER,
    EDUCATION                INTEGER,
    EMPLOYERNAME             VARCHAR(128),
    FIRSTNAME                VARCHAR(64),
    GENDER                   INTEGER,
    INDIVIDUALCLASSIFICATION INTEGER,
    LASTNAME                 VARCHAR(64),
    MARITALSTATUS            INTEGER,
    MIDDLENAMES              VARCHAR(64),
    NATIONALITY              INTEGER,
    NEGATIVESTATUSOFCLIENT   INTEGER,
    NUMBEROFCHILDREN         INTEGER,
    NUMBEROFSPOUSES          INTEGER,
    PROFESSION               INTEGER,
    X__SPOUSEFULLNAMELIST    SMALLINT default 1,
    REPORTING_DATE           DATE
);

