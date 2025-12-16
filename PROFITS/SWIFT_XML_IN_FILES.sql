create table SWIFT_XML_IN_FILES
(
    XML_BIZMSGIDR  VARCHAR(50)  not null,
    SENDER_BIC     CHAR(11)     not null,
    APPHDR_FILE_ID DECIMAL(15)  not null,
    TMSTAMP        TIMESTAMP(6) not null,
    TRX_DATE       DATE,
    FILE_NAME      CHAR(250),
    constraint PK_SWIFT_XML_FILE
        primary key (XML_BIZMSGIDR, SENDER_BIC)
);

