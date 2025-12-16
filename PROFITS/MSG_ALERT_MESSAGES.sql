create table MSG_ALERT_MESSAGES
(
    ID                  DECIMAL(10) not null
        constraint IXM_ALM_000
            primary key,
    FROM_USERNAME       VARCHAR(20),
    TO_USERNAME         VARCHAR(20),
    SENT_TIMESTAMP      TIMESTAMP(6),
    SUBJECT             VARCHAR(100),
    ATTACHMENT_FILENAME VARCHAR(50),
    REQUIRE_ANSWER      SMALLINT,
    ANSWERED            SMALLINT,
    IMPORTANT           SMALLINT,
    CONVERSATION_ID     VARCHAR(100),
    UNREAD              SMALLINT,
    MESSAGE             BLOB(104857600),
    ATTACHMENT          BLOB(104857600)
);

