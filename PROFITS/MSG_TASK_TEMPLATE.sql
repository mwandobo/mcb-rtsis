create table MSG_TASK_TEMPLATE
(
    FK_TASK_ID               DECIMAL(12) not null,
    FK_RECIP_REP_ID          DECIMAL(12) not null,
    FK_LANGUAGE_ID           SMALLINT    not null
        constraint FK_LNG2
            references MSG_LANGUAGE,
    FK_CHANNEL_ID            SMALLINT    not null,
    SHORT_MESSAGE            VARCHAR(2000),
    LONG_MSG_FILENAME        VARCHAR(100),
    TEMPLATE_TYPE            SMALLINT,
    ATTACHMENT_ID            DECIMAL(12)
        constraint FK_ATTCHMNT0
            references MSG_ATTACHMENT,
    DISABLE_FILE_ATTACHMENTS SMALLINT default 0,
    LONG_MESSAGE             BLOB(104857600),
    constraint IXM_TTM_001
        primary key (FK_TASK_ID, FK_RECIP_REP_ID, FK_LANGUAGE_ID, FK_CHANNEL_ID)
);

