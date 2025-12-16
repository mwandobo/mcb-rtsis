create table TEMP_SWIFT_ANAL
(
    USER_CODE        CHAR(8)  not null,
    ERROR_FLAG       CHAR(1)  not null,
    SN               INTEGER  not null,
    TAG              CHAR(10) not null,
    SUBTAG_SN        SMALLINT,
    TAG_LABEL        CHAR(40),
    TAG_FIELD_LENGTH INTEGER,
    TAG_MANDATORY    CHAR(1),
    EXCEPTION_CODE   CHAR(10),
    ENTER_CHARACTER  CHAR(100),
    ERROR_COMMENTS   VARCHAR(100),
    constraint PK_TMP_SWT_ANAL
        primary key (USER_CODE, ERROR_FLAG, SN)
);

