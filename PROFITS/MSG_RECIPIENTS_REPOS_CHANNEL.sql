create table MSG_RECIPIENTS_REPOS_CHANNEL
(
    FK_RECIP_REP_ID      DECIMAL(12) not null,
    FK_CHANNEL_ID        SMALLINT    not null,
    RECIPIENT_SQL        VARCHAR(2000),
    COLUMN_NUM           SMALLINT default 0,
    ADDITIONAL_RECIPIENT VARCHAR(4000),
    constraint IXM_RRC_001
        primary key (FK_RECIP_REP_ID, FK_CHANNEL_ID)
);

