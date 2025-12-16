create table MSG_CHANNEL_PARAM
(
    CHANNEL_ID  SMALLINT      not null,
    PARAM_KEY   VARCHAR(50)   not null,
    PARAM_VALUE VARCHAR(2000) not null,
    constraint IXM_CHP_001
        primary key (CHANNEL_ID, PARAM_KEY)
);

