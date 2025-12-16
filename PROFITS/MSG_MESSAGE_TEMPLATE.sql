create table MSG_MESSAGE_TEMPLATE
(
    FK_SQL_REP_ID     DECIMAL(12)           not null
        constraint FK_SQLRP2
            references MSG_SQL_REPOSITORY,
    FK_CHANNEL_ID     SMALLINT              not null,
    FK_LANGUAGE_ID    SMALLINT              not null
        constraint FK_LNG1
            references MSG_LANGUAGE,
    FK_RECIP_REP_ID   DECIMAL(12) default 0 not null,
    SHORT_MESSAGE     VARCHAR(2000),
    TEMPLATE_TYPE     SMALLINT,
    TEMPLATE_FILENAME VARCHAR(255),
    LONG_MESSAGE      BLOB(104857600),
    constraint IXM_TPL_001
        primary key (FK_SQL_REP_ID, FK_CHANNEL_ID, FK_LANGUAGE_ID, FK_RECIP_REP_ID)
);

