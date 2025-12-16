create table AUTHORISATION_CASE
(
    CODE         CHAR(8)      not null
        constraint PKAUTHO
            primary key,
    TMSTAMP      TIMESTAMP(6) not null,
    DESCRIPTION  CHAR(250)    not null,
    ENTRY_STATUS CHAR(1)
);

