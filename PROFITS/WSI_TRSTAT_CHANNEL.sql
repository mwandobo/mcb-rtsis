create table WSI_TRSTAT_CHANNEL
(
    CHANNEL_PROFITS_ID INTEGER     not null,
    WS_ID              VARCHAR(20) not null,
    WS_COMMAND         CHAR(80)    not null,
    TRX_EXECUTED       DECIMAL(12),
    constraint PK_WSI_1001
        primary key (WS_COMMAND, WS_ID, CHANNEL_PROFITS_ID)
);

