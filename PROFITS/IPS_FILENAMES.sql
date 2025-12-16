create table IPS_FILENAMES
(
    FILENAME       CHAR(50) not null
        constraint IPS_FILENAMES_PK
            primary key,
    PROCESS_STATUS CHAR(1),
    PROCESS_DATE   DATE,
    TMSTAMP        TIMESTAMP(6),
    IPS_FILENAME   CHAR(50),
    PROCESS_DESC   CHAR(80),
    GROUP_ID       DECIMAL(10)
);

