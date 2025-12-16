create table WS_CONFIGURATION
(
    WS_CODE            VARCHAR(20)  not null,
    WS_ADDRESS         VARCHAR(254) not null,
    WS_CLASSNAME       VARCHAR(254) not null,
    ENTRY_STATUS       CHAR(1),
    INPUT_AMOUNT       CHAR(1),
    INPUT_AMOUNT_LABEL VARCHAR(50),
    ERROR_STATUS_LABEL VARCHAR(50),
    WS_PROMPT_1        CHAR(30),
    WS_PROMPT_2        CHAR(30),
    WS_PROMPT_3        CHAR(30),
    WS_PROMPT_4        CHAR(30),
    PARTIAL_PAYMENT    CHAR(1),
    VALIDATE_AMOUNT    CHAR(1),
    WSPOST_CLASS       CHAR(1) default '0',
    constraint PK_WS_CONFIGURATION
        primary key (WS_CLASSNAME, WS_ADDRESS, WS_CODE)
);

