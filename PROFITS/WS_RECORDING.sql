create table WS_RECORDING
(
    TRX_UNIT         INTEGER     not null,
    TRX_DATE         DATE        not null,
    TRX_USR          CHAR(8)     not null,
    TRX_USR_SN       INTEGER     not null,
    TERMINAL_CODE    CHAR(20),
    WS_CODE          VARCHAR(20) not null,
    INPUT_PARAMETERS VARCHAR(4000),
    OUTPUT_RESULT    VARCHAR(4000),
    constraint PK_WS_RECORDING
        primary key (TRX_USR_SN, TRX_USR, TRX_DATE, TRX_UNIT)
);

