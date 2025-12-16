create table LMS_REQ_HEADER
(
    LMS_BANKS_SN       DECIMAL(10)   not null,
    TMSTAMP            TIMESTAMP(6)  not null,
    SN                 DECIMAL(10)   not null,
    IN_REQUEST_ENCRYPT VARCHAR(2048),
    IN_REQUEST_DECRYPT VARCHAR(1024),
    OUT_CODE_ENCRYPT   CHAR(250),
    OUT_CODE_DECRYPT   CHAR(240),
    OUT_CODE_LENGTH    INTEGER       not null,
    OUT_CODE_SENT      VARCHAR(1000) not null,
    OUT_CODE_UNIQUE_ID VARCHAR(16),
    PROCESS_STATUS     CHAR(1),
    LICENSE_COMMENTS   VARCHAR(500),
    constraint PK_LMS_3
        primary key (LMS_BANKS_SN, TMSTAMP, SN)
);

