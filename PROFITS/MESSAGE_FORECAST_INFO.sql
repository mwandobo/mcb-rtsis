create table MESSAGE_FORECAST_INFO
(
    SUBTAG           SMALLINT     not null,
    MESSAGE_SN       INTEGER      not null,
    TAG              CHAR(10)     not null,
    MESSAGE_TYPE     CHAR(20)     not null,
    TMSTAMP          TIMESTAMP(6) not null,
    TAG_FIELD_LENGTH INTEGER,
    VALUE_AMOUNT     DECIMAL(18, 4),
    VALUE_DATE       DATE,
    VALUE_TIMESTAMP  TIMESTAMP(6),
    TAG_FIELD_TYPE   CHAR(2),
    TAG_FIELD_FORMAT CHAR(40),
    ORIGINAL_DATA    CHAR(200),
    VALUE_TEXT       VARCHAR(200),
    FILE_LINE        INTEGER,
    VALUE_TIME       TIME,
    constraint IXU_CP_098
        primary key (SUBTAG, MESSAGE_SN, TAG, MESSAGE_TYPE, TMSTAMP)
);

