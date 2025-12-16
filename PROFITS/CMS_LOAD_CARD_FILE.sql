create table CMS_LOAD_CARD_FILE
(
    EXPIRY_DATE    CHAR(4),
    PAN            CHAR(19),
    FILE_SN        INTEGER not null,
    LINE_NO        INTEGER not null,
    TRX_DATE       DATE,
    REC_TYPE       CHAR(1),
    TOTAL_DTL_RECS INTEGER,
    CARD_PREFIX    VARCHAR(11),
    FULL_LINE      VARCHAR(1000),
    TMSTAMP        TIMESTAMP(6),
    constraint CMS_LOAD_CARD_FILE_PK
        primary key (FILE_SN, LINE_NO)
);

