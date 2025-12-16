create table LMS_BANK_PARAM
(
    BANK_CODE     SMALLINT    not null
        constraint PK_LMS_2
            primary key,
    REQUESTS_PATH CHAR(40),
    EMAIL_ADDRESS CHAR(254),
    EMAIL_CC_1    CHAR(254),
    EMAIL_CC_2    CHAR(254),
    EMAIL_CC_3    CHAR(254),
    EMAIL_CC_4    CHAR(254),
    EMAIL_CC_5    CHAR(254),
    EMAIL_CC_6    CHAR(254),
    EMAIL_CC_7    CHAR(254),
    EMAIL_CC_8    CHAR(254),
    EMAIL_CC_9    CHAR(254),
    EMAIL_CC_10   CHAR(254),
    EMAIL_BODY    VARCHAR(4000),
    EMAIL_SUBJECT CHAR(254),
    USER_TYPE     CHAR(1),
    LMS_BANKS_SN  DECIMAL(10) not null,
    DTS           SMALLINT,
    WES           CHAR(3)
);

