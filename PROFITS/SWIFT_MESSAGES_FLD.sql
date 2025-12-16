create table SWIFT_MESSAGES_FLD
(
    PRFT_REF_NO     CHAR(16)     not null,
    TRX_REF_NO_20   CHAR(16)     not null,
    MSG_TYPE        CHAR(20)     not null,
    MSG_CATEGORY    CHAR(1)      not null,
    MESSAGE_SN      INTEGER      not null,
    SN              INTEGER      not null,
    TAG             CHAR(10)     not null,
    SUBTAG_SN       SMALLINT     not null,
    FIELD_NAME      VARCHAR(40)  not null,
    FIELD_TYPE      CHAR(2),
    VALUE_TEXT      VARCHAR(150) not null,
    VALUE_DATE      DATE,
    VALUE_NUMBER    DECIMAL(15, 2),
    VALUE_TIMESTAMP TIMESTAMP(6),
    VALUE_FLAG      VARCHAR(2),
    FORMATTED_VALUE CHAR(150),
    VALUE_TIME      TIME,
    constraint PK_SWT_MSG_FLD
        primary key (SUBTAG_SN, TAG, SN, PRFT_REF_NO)
);

