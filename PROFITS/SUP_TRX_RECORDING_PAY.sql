create table SUP_TRX_RECORDING_PAY
(
    TRX_DATE            DATE       not null,
    TRX_UNIT            DECIMAL(5) not null,
    TRX_USER            CHAR(8)    not null,
    TRX_USR_SN          DECIMAL(8) not null,
    TRX_USR_INT_SN      DECIMAL(2) not null,
    PAYMENT_SN          DECIMAL(2) not null,
    PAYMENT_METHOD      DECIMAL(5) not null,
    SCHED_PAYMENT_DT    DATE,
    FC_AMOUNT           DECIMAL(18, 2),
    DC_AMOUNT           DECIMAL(18, 2),
    FK_ID_CURRENCY      DECIMAL(5),
    STATUS              DECIMAL(5),
    COMMENTS            VARCHAR(200),
    REMAINING_AMOUNT_FC DECIMAL(18, 2),
    constraint IXU_SUP_TRX_PAY
        primary key (TRX_DATE, TRX_UNIT, TRX_USER, TRX_USR_SN, TRX_USR_INT_SN, PAYMENT_SN)
);

