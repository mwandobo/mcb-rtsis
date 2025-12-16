create table OUTGOING_SWIFT_DATA
(
    ORDER_NUMBER          INTEGER     not null
        primary key,
    SENDER_ID             INTEGER     not null,
    SENDER_ACCOUNT_NUMBER CHAR(35)    not null,
    SENDER_NAME           VARCHAR(40) not null,
    SENDER_TYPE           CHAR(1)     not null,
    BRANCH_NUMBER         INTEGER     not null,
    SENDER_DATE_OF_BIRTH  DATE,
    SENDER_TAX_ID         VARCHAR(20),
    SENDER_ADDRESS1       VARCHAR(40) not null,
    SENDER_ADDRESS2       VARCHAR(40),
    SENDER_CITY           VARCHAR(30) not null,
    SENDER_ZIP_CODE       CHAR(10)    not null,
    SENDER_COUNTRY        VARCHAR(40) not null,
    SENDER_BIC_CODE       CHAR(11)    not null,
    RECEIVER_BIC_CODE     CHAR(11)    not null,
    SENDER_PHONE          VARCHAR(20) not null,
    SENDER_ID_NUM         VARCHAR(20) not null,
    SENDER_ID_TYPE        CHAR(1),
    TRX_DATE              DATE        not null,
    VALUE_DATE            DATE        not null,
    DESCRIPTION           VARCHAR(235)
);

