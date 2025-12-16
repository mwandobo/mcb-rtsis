create table MOF_SEIZURE_DETAIL
(
    MOF_GGPS_PROTOCOL_DT  DATE     not null,
    MOF_GGPS_PROTOCOL_NO  CHAR(18) not null,
    INTERNAL_SN           INTEGER  not null,
    ACCOUNT_NUMBER        CHAR(40),
    CUST_ID               INTEGER,
    SEIZURE_TOT_AMNT      DECIMAL(15, 2),
    ORDER_AMOUNT          DECIMAL(15, 2),
    REMAINING_AMOUNT      DECIMAL(15, 2),
    ORDER_CODE            CHAR(20),
    PREV_REMAINING_AMOUNT DECIMAL(15, 2),
    TRX_UNIT              INTEGER  not null,
    TRX_DATE              DATE     not null,
    TRX_USR               CHAR(8)  not null,
    TRX_USR_SN            INTEGER  not null,
    TMSTAMP               TIMESTAMP(6),
    COMMENTS              CHAR(40),
    constraint PK_SEIZ_DETAI
        primary key (MOF_GGPS_PROTOCOL_DT, MOF_GGPS_PROTOCOL_NO, INTERNAL_SN)
);

