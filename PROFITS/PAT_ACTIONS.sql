create table PAT_ACTIONS
(
    TA_ID                       INTEGER  not null
        constraint PATACPK1
            primary key,
    PRFT_SYSTEM                 INTEGER  not null,
    CODE1                       INTEGER,
    TAG                         CHAR(40),
    STATUS                      CHAR(1)  not null,
    DEFAULT_ERROR_HANDLING_TYPE CHAR(3),
    TIME_INTERVAL               INTEGER,
    SUBCODE                     SMALLINT not null
);

