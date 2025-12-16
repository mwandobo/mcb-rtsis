create table FILE_TO_DB_DTL
(
    FILE_CHUNK INTEGER     not null,
    FILE_SN    DECIMAL(15) not null,
    BUFF_SIZE  INTEGER,
    BUFF       VARCHAR(4000),
    constraint IXU_CP_093
        primary key (FILE_CHUNK, FILE_SN)
);

