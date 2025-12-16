create table RSKCO_ERROR_LOG
(
    CREATION_DATE  TIMESTAMP(6) not null
        constraint IXU_LNS_043
            primary key,
    PROGRAM_ID     CHAR(5),
    FIELD_AFFECTED CHAR(40),
    ERROR_DESCR    CHAR(80)
);

