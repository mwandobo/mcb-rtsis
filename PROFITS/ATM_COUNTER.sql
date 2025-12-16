create table ATM_COUNTER
(
    TRX_DATE           DATE    not null,
    ATM_COUNTER        INTEGER not null,
    STAND_IN_FLAG      CHAR(1) not null,
    ALPHA_PREFIX       CHAR(1) not null,
    TRX_UNIT           INTEGER,
    USER_SN            INTEGER,
    ATM_DATE           DATE,
    ATM_TIME           DATE,
    TRANSACTION_STATUS CHAR(1),
    MTI_CODE           CHAR(4),
    TRX_USER           CHAR(8),
    REFERENCE_NUMBER   CHAR(12),
    CARD_ACCEPTOR      CHAR(16),
    TRACK_DATA         CHAR(37),
    constraint IXU_ATM_032
        primary key (TRX_DATE, ATM_COUNTER, STAND_IN_FLAG, ALPHA_PREFIX)
);

