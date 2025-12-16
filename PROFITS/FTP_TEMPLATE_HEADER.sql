create table FTP_TEMPLATE_HEADER
(
    TEMPLATE_ID       INTEGER  not null
        constraint PK_FTP_TEMPLATE_HEADER
            primary key,
    NAME              CHAR(50),
    DESCRIPTION       VARCHAR(200),
    DATA_ROW_START    INTEGER,
    ENABLED           CHAR(1),
    MECHANISM_CODE    SMALLINT not null,
    GROUP_COLUMN_NUM  SMALLINT not null,
    INSTRUCTION_GROUP VARCHAR(20),
    MECHANISM_COLUMN  INTEGER,
    FILE_FORMAT       CHAR(3)
);

