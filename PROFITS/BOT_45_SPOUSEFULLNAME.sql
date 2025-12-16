create table BOT_45_SPOUSEFULLNAME
(
    SPOUSEFULLNAME_ID INTEGER generated always as identity
        constraint BOT_45_SPOUSEFULLNAME_ID_PK
            primary key,
    SPOUSEFULLNAME    VARCHAR(128),
    REPORTING_DATE    DATE
);

