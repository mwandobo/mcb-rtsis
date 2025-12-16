create table FLD_DESTINATION_DETAILS
(
    DESTINATION_ID            INTEGER                 not null,
    DATA_COLUMN_NUM           INTEGER                 not null,
    DESTINATION_COLUMN_NAME   VARCHAR(50)             not null,
    DESTINATION_COLUMN_TYPE   INTEGER                 not null,
    CREATED_DATE              TIMESTAMP(6)            not null,
    UPDATED_DATE              TIMESTAMP(6)            not null,
    CREATED_BY                VARCHAR(10) default '0' not null,
    UPDATED_BY                VARCHAR(10) default '0' not null,
    FORMAT                    VARCHAR(200),
    IS_MANDATORY              SMALLINT    default 0   not null,
    TRIM_START_END            SMALLINT    default 0   not null,
    DESTINATION_COLUMN_LENGTH INTEGER                 not null,
    OUTPUT_LENGTH             INTEGER                 not null,
    LEFT_PADDING_CHARACTER    VARCHAR(1),
    STARTING_POSITION         INTEGER,
    INPUT_DECIMAL_SEPARATOR   VARCHAR(1),
    INPUT_DECIMAL_PLACES      INTEGER,
    DECIMAL_PLACES            INTEGER,
    constraint FK_DEST_DETAILS
        primary key (DESTINATION_ID, DATA_COLUMN_NUM)
);

