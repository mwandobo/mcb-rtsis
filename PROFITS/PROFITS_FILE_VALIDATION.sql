create table PROFITS_FILE_VALIDATION
(
    FILENAME       CHAR(50)    not null,
    PRFT_SYSTEM    CHAR(2)     not null,
    TRANSACT_ID    DECIMAL(10) not null,
    PROCESS_DATE   DATE,
    TMSTAMP        TIMESTAMP(6),
    PROCESS_STATUS CHAR(1),
    constraint PRFT_FILENAMES_PK
        primary key (TRANSACT_ID, PRFT_SYSTEM, FILENAME)
);

