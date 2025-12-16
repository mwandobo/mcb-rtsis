create table OTP_USER
(
    CHANNEL_ID          INTEGER      not null,
    EXT_USER            VARCHAR(100) not null,
    EXT_KEY             VARCHAR(100) not null,
    LST_SYSTEM_DT       DATE,
    LST_TMSTAMP         TIMESTAMP(6),
    USR_STATUS          CHAR(1),
    LST_KEY_CREATED     VARCHAR(40),
    LST_KEY_USED        VARCHAR(40),
    REGISTER_DT         TIMESTAMP(6),
    REGISTER_FLG        CHAR(1),
    REGISTER_EMAIL      VARCHAR(80),
    REGISTER_MOBILE     VARCHAR(80),
    REGISTER_FIRST_NAME VARCHAR(100),
    REGISTER_SURNAME    VARCHAR(100),
    REGISTER_DOB        DATE,
    REGISTER_UPDATE     TIMESTAMP(6),
    REGISTER_KEY_TYPE   CHAR(2),
    CBS_CUSTOMER_ID     DECIMAL(10),
    CBS_USER_ID         CHAR(40),
    SEND_EMAIL_OTP      CHAR(1),
    SEND_SMS_OTP        CHAR(1),
    USR_COMMENTS        VARCHAR(500),
    constraint PK_OTPUSER
        primary key (EXT_KEY, EXT_USER, CHANNEL_ID)
);

