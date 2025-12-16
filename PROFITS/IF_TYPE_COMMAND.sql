create table IF_TYPE_COMMAND
(
    SERIAL_NO          INTEGER      not null
        constraint PIFTYPEC
            primary key,
    DESCRIPTION        CHAR(250)    not null,
    REQUIRED_PROFILE_1 CHAR(8)      not null,
    REQUIRED_PROFILE_2 CHAR(8),
    PROFILE_LEVEL      CHAR(1),
    FK_AUTHORISATIOCOD CHAR(8),
    FK_SEC_RULECODE    INTEGER,
    ENTRY_STATUS       CHAR(1),
    TMSTAMP            TIMESTAMP(6) not null
);

