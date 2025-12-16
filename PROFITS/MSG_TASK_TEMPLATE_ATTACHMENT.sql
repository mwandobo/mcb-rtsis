create table MSG_TASK_TEMPLATE_ATTACHMENT
(
    FK_TASK_ID           DECIMAL(12) not null
        constraint FK_TSK5
            references MSG_TASK,
    FK_RECIP_REP_ID      DECIMAL(12) not null
        constraint FK_RREP5
            references MSG_RECIPIENTS_REPOSITORY,
    FK_LANGUAGE_ID       SMALLINT    not null
        constraint FK_LNG3
            references MSG_LANGUAGE,
    FK_CHANNEL_ID        SMALLINT    not null
        constraint FK_CHNNL8
            references MSG_CHANNEL,
    SN                   SMALLINT    not null,
    ATTACHMENT_TYPE      SMALLINT default 0,
    FK_ATTCAMENT_ID      DECIMAL(12)
        constraint FK_ATTCHMNT1
            references MSG_ATTACHMENT,
    FK_FILE_FIELD_ID     SMALLINT,
    SEASONAL             SMALLINT default 0,
    SEASON_STARTS        TIMESTAMP(6),
    SEASON_ENDS          TIMESTAMP(6),
    STATUS               SMALLINT default 0,
    ZIPPED               SMALLINT default 0,
    FK_PASSWORD_FIELD_ID SMALLINT,
    LABEL                VARCHAR(20)
);

create unique index IXM_TTA_001
    on MSG_TASK_TEMPLATE_ATTACHMENT (FK_TASK_ID, FK_RECIP_REP_ID, FK_LANGUAGE_ID, FK_CHANNEL_ID, SN);

