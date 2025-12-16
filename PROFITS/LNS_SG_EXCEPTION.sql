create table LNS_SG_EXCEPTION
(
    PROGRAM_ID     CHAR(5) not null,
    EXCEPTION_TYPE CHAR(5) not null,
    SERIAL_NUMBER  INTEGER not null,
    EXC_NUMBER     INTEGER,
    EXC_TEXT       CHAR(30),
    EXC_AMOUNT     DECIMAL(15, 2),
    EXC_DATE       DATE,
    constraint IXU_LOA_103
        primary key (PROGRAM_ID, SERIAL_NUMBER, EXCEPTION_TYPE)
);

