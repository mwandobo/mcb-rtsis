create table CMS_ORD_INVENTORY
(
    CARD_INV_ID          DECIMAL(10) not null
        constraint PK_CARD_ORD_INVENT
            primary key,
    SEQUENCE_NUMBER      SMALLINT,
    PAN                  CHAR(19),
    CARD_ORDER_ID        DECIMAL(10),
    BIN_NUMBER           CHAR(8),
    INDIVIDUAL_ACC_ID    CHAR(12),
    CHECK_DIGIT          CHAR(1),
    LAST_4_DIGITS        CHAR(4),
    REJECTION_REASON     CHAR(80),
    CARD_STATUS          CHAR(1),
    EXPIRY_DATE          CHAR(4),
    ASSIGNED_UNIT        INTEGER,
    ASSIGNED_DATE        DATE,
    ASSIGNED_APPLICATION DECIMAL(10),
    CANCEL_DATE          DATE,
    CANCEL_TIME          TIMESTAMP(6),
    CANCEL_USER          CHAR(8),
    CANCEL_AUTH_USER     CHAR(8),
    CANCEL_REASON        CHAR(80),
    TMSTAMP              TIMESTAMP(6)
);

comment on column CMS_ORD_INVENTORY.CARD_STATUS is '1 -Available 2 -Rejected  3 -Active';

