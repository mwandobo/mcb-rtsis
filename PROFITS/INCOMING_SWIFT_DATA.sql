create table INCOMING_SWIFT_DATA
(
    PRFT_REF_NO          CHAR(16) not null
        primary key,
    SENDER_ID            VARCHAR(2048),
    ACC_NUM_OR_IBAN      VARCHAR(2048),
    SENDER_NAME          CHAR(70),
    SENDER_DATE_OF_BIRTH DATE,
    SENDER_TAX_ID        CHAR(20),
    SENDER_ADDRESS1      VARCHAR(40),
    SENDER_ADDRESS2      VARCHAR(40),
    CITY_AND_ZIP_CODE    CHAR(30),
    SENDER_COUNTRY       VARCHAR(40),
    SENDER_BIC_CODE      CHAR(11) not null,
    RECEIVER_BIC_CODE    CHAR(11) not null,
    SENDER_ID_NUM        CHAR(20),
    ISSUER_INFO_50       VARCHAR(2048),
    TRX_DATE             DATE     not null,
    VALUE_DATE           DATE     not null,
    DESCRIPTION          VARCHAR(100)
);

