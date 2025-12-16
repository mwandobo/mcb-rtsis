create table MAND_OTP_WINDOW_FIELD
(
    ATTR_CODE      INTEGER      not null,
    WIND_CODE      INTEGER      not null,
    PROMPT         VARCHAR(200) not null,
    VIEW_NAME      VARCHAR(40)  not null,
    ATTRIBUTE      VARCHAR(40)  not null,
    ALT_PROMPT     VARCHAR(60),
    DEFAULT_PROMPT VARCHAR(50),
    constraint IXU_DCD_047
        primary key (ATTR_CODE, WIND_CODE)
);

