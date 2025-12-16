create table ATM_CSV_FILE_FORMAT
(
    FORMAT_ID      INTEGER  not null,
    FIELD_NO       SMALLINT not null,
    FIELD_1_TYPE   CHAR(1),
    FIELD_DECIMALS INTEGER,
    DATE_FORMAT    CHAR(30),
    constraint ATM_CSV_FILE_FORMAT_PK
        primary key (FIELD_NO, FORMAT_ID)
);

