create table WSI_CHANNEL_SEQUENCE
(
    ID_CHANNEL    INTEGER not null,
    USERS_COUNT   DECIMAL(15),
    TMSTAMP       TIMESTAMP(6),
    USER_SEQUENCE DECIMAL(15) generated always as identity,
    constraint PK_CHAN_SEQUENCE
        primary key (USER_SEQUENCE, ID_CHANNEL)
);

