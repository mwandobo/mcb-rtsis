create table WSI_USER_KEY
(
    CHANNEL_USER   VARCHAR(40) not null,
    CHANNEL_ID     INTEGER     not null,
    CURR_TRX_DATE  DATE        not null,
    USER_SN        INTEGER,
    CREATE_TMSTAMP TIMESTAMP(6),
    UPDATE_TMSTAMP TIMESTAMP(6),
    constraint PK_WSI_1005
        primary key (CURR_TRX_DATE, CHANNEL_USER, CHANNEL_ID)
);

