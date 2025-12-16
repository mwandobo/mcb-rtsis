create table PAT_GENVALUE_TYPES
(
    GVT_ID                  CHAR(5)       not null
        constraint PATVTPK1
            primary key,
    CREATION_TIME_INDICATOR CHAR(1)       not null,
    PROVIDING_SYSTEM        CHAR(5),
    DESCRIPTION             CHAR(240),
    SCRIPT_PATH             VARCHAR(2000) not null
);

