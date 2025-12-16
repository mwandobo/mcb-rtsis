create table MSG_TASK_RECIPIENTS
(
    FK_TASK_ID           DECIMAL(12) not null,
    FK_RECIP_REP_ID      DECIMAL(12) not null,
    FK_CHANNEL_ID        SMALLINT    not null,
    AUTOAPPROUVE         SMALLINT default 0,
    ADDITIONAL_RECIPIENT VARCHAR(4000),
    constraint IXM_TRP_001
        primary key (FK_TASK_ID, FK_RECIP_REP_ID, FK_CHANNEL_ID)
);

