create table BOT_69_EXPENDITURES
(
    EXPENDITURES_ID INTEGER generated always as identity
        constraint BOT_69_EXPENDITURES_ID_PK
            primary key,
    FK_PERSONALDATA INTEGER
        constraint BOT_69_FKPERSONALDATA
            references BOT_63_PERSONALDATA,
    VALUE           DECIMAL(19, 4),
    CURRENCY        INTEGER,
    REPORTING_DATE  DATE
);

