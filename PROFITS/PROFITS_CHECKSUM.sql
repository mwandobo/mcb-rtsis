create table PROFITS_CHECKSUM
(
    FILENAME          VARCHAR(100) not null,
    IMPORT_DATE       VARCHAR(19)  not null,
    TYPE              VARCHAR(10)  not null,
    CHECKSUM          VARCHAR(50),
    MODIFICATION_DATE VARCHAR(19),
    primary key (IMPORT_DATE, TYPE, FILENAME)
);

