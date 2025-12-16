create table MSG_TASK_CHANNEL
(
    FK_TASK_ID               DECIMAL(12) not null,
    FK_CHANNEL_ID            SMALLINT    not null,
    FK_PRIORITY              SMALLINT    not null
        constraint FK_PRRT2
            references MSG_PRIORITY,
    FK_IMPORTANCE_LVL        SMALLINT    not null,
    SEND_QUEUE_FLG           SMALLINT    default 0,
    STATUS                   SMALLINT    default 0,
    DAYS_FREQUENCY           SMALLINT    not null,
    TIME_FREQUENCY           TIME        not null,
    PREFERRED_SENDING_TIME   TIME,
    FK_RESPONSE_CHANNEL_ID   SMALLINT    default 0,
    FK_RESPONSE_RECIPIENT_ID DECIMAL(12) default 0,
    constraint IXM_TCH_001
        primary key (FK_TASK_ID, FK_CHANNEL_ID)
);

