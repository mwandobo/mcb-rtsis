create table CP_ATE_ADVANCE
(
    CUSTOMER_ID       INTEGER     not null,
    PAYMENT_DATE      DATE        not null,
    CP_AGREEMENT_NO   DECIMAL(10) not null,
    PAYM_PERIOD       CHAR(6)     not null,
    ATE_CUST_ID       INTEGER,
    CUST_ACCOUNT      DECIMAL(11),
    INTERNAL_SN       DECIMAL(13),
    TP_AMOUNT         DECIMAL(15, 2),
    RETURNED_AMOUNT   DECIMAL(15, 2),
    RETURNED_DATE     DATE,
    TIMESTMP          TIMESTAMP(6),
    ADVANCE_TYPE      CHAR(1),
    ENTRY_STATUS      CHAR(1),
    DATA_FIELD_3      CHAR(80),
    DATA_FIELD_2      CHAR(80),
    DATA_FIELD_1      CHAR(80),
    DATA_FIELD_4      CHAR(80),
    ERROR_DESCRIPTION CHAR(40),
    constraint IXU_CP_079
        primary key (CUSTOMER_ID, PAYMENT_DATE, CP_AGREEMENT_NO, PAYM_PERIOD)
);

