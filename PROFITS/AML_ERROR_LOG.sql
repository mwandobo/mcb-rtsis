create table AML_ERROR_LOG
(
    CREATION_DATE TIMESTAMP(6) not null
        constraint IXU_AML_009
            primary key,
    PROGRAM_ID    CHAR(5),
    KEY           CHAR(40),
    ERROR_DESCR   CHAR(80)
);

create unique index AML_ERR_001
    on AML_ERROR_LOG (PROGRAM_ID, KEY, CREATION_DATE);

