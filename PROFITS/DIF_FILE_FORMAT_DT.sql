create table DIF_FILE_FORMAT_DT
(
    FIELD_SECTION      SMALLINT not null,
    FIELD_SN           INTEGER  not null,
    FIELD_USAGE        SMALLINT,
    USAGE_VALUE        VARCHAR(100),
    LITERAL_DATA       VARCHAR(200),
    FIELD_TYPE         CHAR(2),
    FIELD_ROW          SMALLINT,
    START_POSITION     INTEGER,
    FIELD_LENGTH       INTEGER,
    DEC_PLACES         INTEGER,
    DATE_FORMAT        CHAR(10),
    TIMESTAMP_FORMAT   CHAR(26),
    TIME_FORMAT        CHAR(10),
    THOUSAND_SEP       CHAR(1),
    DECIMAL_SEP        CHAR(1),
    SIGN_POS           CHAR(1),
    FIELD_CONTINUE     CHAR(1),
    TMSTAMP            TIMESTAMP(6),
    LINE_BREAK         CHAR(1),
    FK_DIF_FILE_FORFOR CHAR(5)  not null,
    FK_DFM_STRCTUSE    INTEGER,
    FK_DFM_STRUCTID    INTEGER,
    GROUP_SN           SMALLINT,
    REC_SIGN           CHAR(1),
    OPTIONAL_FLG       CHAR(1),
    constraint IXU_DFM_003
        primary key (FK_DIF_FILE_FORFOR, FIELD_SECTION, FIELD_SN)
);

