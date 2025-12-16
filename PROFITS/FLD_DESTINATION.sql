create table FLD_DESTINATION
(
    ID              INTEGER,
    TABLE_NAME      VARCHAR(50)             not null,
    DESCRIPTION     VARCHAR(4000)           not null,
    CREATED_DATE    TIMESTAMP(6)            not null,
    UPDATED_DATE    TIMESTAMP(6)            not null,
    CREATED_BY      VARCHAR(10) default '0' not null,
    UPDATED_BY      VARCHAR(10) default '0' not null,
    IMPORTED_DATE   TIMESTAMP(6)            not null,
    STARTING_ROW    INTEGER     default 0   not null,
    STARTING_COLUMN INTEGER     default 0   not null,
    COLUMNS_TO_READ INTEGER     default 0   not null,
    FILE_TYPE       INTEGER     default 0   not null,
    DELETED         SMALLINT    default 0   not null
);

