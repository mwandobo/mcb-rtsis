create table SWIFT_RUN_DETAIL
(
    SUBTAG          SMALLINT not null,
    TAG             CHAR(10) not null,
    PRFT_REF_NO     CHAR(16) not null,
    REPETATIVE_REF  INTEGER,
    VALUE_AMOUNT    DECIMAL(18, 4),
    VALUE_DATE      DATE,
    VALUE_TIMESTAMP TIMESTAMP(6),
    FIELD_TYPE      CHAR(2),
    MESSAGE_TYPE    CHAR(20),
    SWIFT_DATA      CHAR(100),
    ERROR_COMMENTS  VARCHAR(100),
    VALUE_TEXT      VARCHAR(200),
    VALUE_TIME      TIME,
    constraint IXU_SWI_011
        primary key (SUBTAG, TAG, PRFT_REF_NO)
);

