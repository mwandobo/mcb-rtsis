create table CP_FILE_IDENTITY
(
    INPUT_FILE_ID    CHAR(20) not null,
    ORGANIZ_FILENAME CHAR(20) not null,
    TMSTAMP          TIMESTAMP(6),
    constraint IXU_CP__028
        primary key (INPUT_FILE_ID, ORGANIZ_FILENAME)
);

