create table LMS_BANK_REQ
(
    BANK_CODE       SMALLINT     not null,
    REQ_TIMESTAMP   TIMESTAMP(6) not null,
    SN              INTEGER      not null,
    CURR_TRX_DATE   DATE,
    BANK_NAME       VARCHAR(40),
    EMAIL_FILENAME  CHAR(250),
    LICENSE_REQUEST VARCHAR(2048),
    LICENSE_STATUS  CHAR(1),
    CBPTSAW         VARCHAR(2048),
    constraint PK_LMS_1
        primary key (SN, BANK_CODE, REQ_TIMESTAMP)
);

