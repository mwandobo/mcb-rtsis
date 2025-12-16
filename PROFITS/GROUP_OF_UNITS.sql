create table GROUP_OF_UNITS
(
    GROUP_ID    INTEGER not null
        constraint IXU_SEC_010
            primary key,
    DESCRIPTION CHAR(40),
    SUBSYSTEM   SMALLINT
);

