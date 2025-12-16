create table MSG_OUTBOX
(
    ID                    DECIMAL(12)  not null
        constraint IXM_OBX_001
            primary key,
    FK_TASK_ID            DECIMAL(12),
    FK_CHANNEL_ID         SMALLINT     not null,
    FK_SOURCE_SYS_ID      INTEGER,
    RECIPIENT             VARCHAR(512) not null,
    SUBJECT               VARCHAR(100) not null,
    SHORT_MESSAGE         BLOB(1073741824),
    STATUS                SMALLINT,
    APPROVAL_FLG          SMALLINT,
    PREFERED_SENDING_TIME TIMESTAMP(6),
    CREATED               TIMESTAMP(6),
    FK_RECIPIENT_ID       DECIMAL(12) default 0,
    KEYDATETIME           TIMESTAMP(6),
    RESPONSE              BLOB(1073741824),
    ORIGINATION_ID        DECIMAL(12),
    SIGNIFICANT_VALUE     VARCHAR(100),
    COUNT_TILL_FAILED     SMALLINT,
    ADDITIONAL_RECIPIENT  VARCHAR(4000),
    EXTERNAL_SYSTEM_ID    VARCHAR(50)
);

create unique index IXM_OBX_002
    on MSG_OUTBOX (FK_CHANNEL_ID, STATUS);

create unique index SC_EXTRAIT_BLOB
    on MSG_OUTBOX (FK_TASK_ID, SIGNIFICANT_VALUE);

