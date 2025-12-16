create table SWIFT_MESSAGES_FLI
(
    PRFT_REF_NO     CHAR(16)    not null,
    TAG             CHAR(10)    not null,
    TAG_SN          DECIMAL(10) not null,
    SUBTAG_SN       SMALLINT,
    TAG_LABEL       CHAR(40),
    MESSAGE_TYPE    CHAR(20)    not null,
    TRX_REF_NO_20   CHAR(16)    not null,
    SENDER_BIC      CHAR(12)    not null,
    ENTER_CHARACTER CHAR(100),
    TAG_TYPE        VARCHAR(10),
    constraint PK_SWT_MSG_TAGS
        primary key (TAG_SN, TAG, PRFT_REF_NO)
);

