create table CMS_CAF_HDR
(
    FILE_SN             DECIMAL(10)  not null
        constraint PK_CMS_CARD_HDR
            primary key,
    TRX_DATE            DATE,
    PROGRAM_ID          CHAR(10),
    FILE_REC_CNT        DECIMAL(10),
    DETAILS_REC_CNT     DECIMAL(10),
    TRAILER_LINE        VARCHAR(1000),
    HEADER_LINE         VARCHAR(1000),
    ORGANIZATION_HEADER VARCHAR(1000),
    ORGANIZATION_DETAIL VARCHAR(1000),
    TMSTAMP             TIMESTAMP(6) not null,
    COMPLETE_FILE_FLAG  CHAR(1)
);

