create table CMS_MERCH_HDR
(
    FILE_SN           INTEGER not null
        constraint CMS_MERCH_HDR_PK1
            primary key,
    LOAD_DATE         DATE    not null,
    ENTRY_STATUS      CHAR(1) not null,
    LOAD_TMSTAMP      TIMESTAMP(6),
    PROCESSED_DATE    DATE,
    PROCESSED_TMSTAMP TIMESTAMP(6),
    REPLY_DATE        DATE,
    REPLY_TMSTAMP     TIMESTAMP(6)
);

