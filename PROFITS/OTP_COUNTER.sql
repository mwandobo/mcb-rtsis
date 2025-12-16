create table OTP_COUNTER
(
    CHANNEL_ID     INTEGER      not null,
    EXT_USER       VARCHAR(100) not null,
    EXT_KEY        VARCHAR(100) not null,
    CURR_TRX_DATE  DATE         not null,
    OTP_SN         DECIMAL(10),
    KEYS_CREATED   DECIMAL(10),
    KEYS_USED      DECIMAL(10),
    UNSUCCES_TRIES INTEGER,
    constraint PK_OTPCNTR
        primary key (CURR_TRX_DATE, EXT_KEY, EXT_USER, CHANNEL_ID)
);

