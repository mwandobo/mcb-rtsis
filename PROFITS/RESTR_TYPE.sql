create table RESTR_TYPE
(
    ID_RESTRTYPE INTEGER not null
        constraint I0000616
            primary key,
    DESCRIPTION  VARCHAR(40),
    ENTRY_STATUS VARCHAR(1),
    TMSTMP       TIMESTAMP(6)
);

