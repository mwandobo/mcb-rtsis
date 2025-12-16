create table SWIFT_LOAD_FILE
(
    LINE_NO       INTEGER  not null,
    PRFT_REF_NO   CHAR(16) not null,
    FIELD_SECTION SMALLINT,
    TRX_DATE      DATE,
    TMSTAMP       TIMESTAMP(6),
    MSG_CATEGORY  CHAR(1),
    MESSAGE_TYPE  CHAR(20),
    FILENAME      CHAR(50),
    FULL_LINE     VARCHAR(2048),
    REPEATING_GRP SMALLINT,
    TAG           CHAR(10),
    GPI_CONTROL   CHAR(1),
    OTHER_REF     CHAR(3),
    constraint IXU_FX_027
        primary key (LINE_NO, PRFT_REF_NO)
);

