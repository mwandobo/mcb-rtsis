create table UTB_UNITUSR_PASS
(
    FK_UNIT    INTEGER not null
        constraint IXU_CP_107
            primary key,
    TIMEOUT    SMALLINT,
    IP_ADDRESS VARCHAR(15),
    USERNAME   VARCHAR(30),
    PASSWORD   VARCHAR(30)
);

