create table DIAS_EXCEPTIONS
(
    PROGRAM_ID   CHAR(5)      not null,
    TMSTAMP      TIMESTAMP(6) not null,
    EXCEPTION    VARCHAR(500),
    ENTRY_STATUS CHAR(1),
    constraint IXU_BIL_103
        primary key (TMSTAMP, PROGRAM_ID)
);

