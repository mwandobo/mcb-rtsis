create table DCD_GEN_DEF
(
    ID          INTEGER not null
        constraint IXU_DEF_116
            primary key,
    COUNTER1    INTEGER,
    COUNTER2    INTEGER,
    COUNTER     INTEGER,
    TYPE        CHAR(1),
    OTHER3VALUE CHAR(5),
    OTHER1VALUE CHAR(20),
    MAIN_VALUE  CHAR(20),
    OTHER2VALUE CHAR(20)
);

