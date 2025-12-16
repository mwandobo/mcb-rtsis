create table SQL_QUERY
(
    KEY_1       DECIMAL(12)  not null,
    KEY_2       DECIMAL(12)  not null,
    KEY_3       DECIMAL(15)  not null,
    KEY_4       VARCHAR(80)  not null,
    TIMESTMP    TIMESTAMP(6) not null,
    MULTI_ROW   SMALLINT     not null,
    INTERNAL_SN DECIMAL(15)  not null,
    SHEET       INTEGER      not null,
    DATA_SQL    VARCHAR(4000),
    RESULT_SQL  VARCHAR(4000),
    CHAR_1      VARCHAR(100),
    CHAR_2      VARCHAR(100),
    CHAR_3      VARCHAR(100),
    CHAR_4      VARCHAR(100),
    constraint PK_SQLQUERY
        primary key (TIMESTMP, KEY_4, KEY_3, KEY_2, KEY_1)
);

