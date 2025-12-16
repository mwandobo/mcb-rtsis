create table IPS_TEMPLATE_HEADER
(
    TEMPLATE_ID         INTEGER not null
        constraint PK_IPS_TEMPLATE_HEADER
            primary key,
    NAME                CHAR(50),
    DESCRIPTION         VARCHAR(200),
    DATA_ROW_START      INTEGER,
    FILE_FORMAT         CHAR(3),
    ENABLED             CHAR(1),
    MECHANISM_CODE      SMALLINT,
    GROUP_COLUMN_NUMBER SMALLINT
);

