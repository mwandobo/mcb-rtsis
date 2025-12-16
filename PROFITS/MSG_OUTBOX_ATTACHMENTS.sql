create table MSG_OUTBOX_ATTACHMENTS
(
    ID              DECIMAL(12) not null
        constraint IXM_OAT_001
            primary key,
    FK_OUTBOX_ID    DECIMAL(12) not null,
    SERIAL_NUMBER   SMALLINT    not null,
    ATTACHMENT_TYPE SMALLINT,
    ATTACHMENT      BLOB(1048576),
    CREATED         TIMESTAMP(6)
);

create unique index IXM_OAT_002
    on MSG_OUTBOX_ATTACHMENTS (FK_OUTBOX_ID);

