create table TRS_MM_DISTR_DTL
(
    PAYMENT_AMOUNT         DECIMAL(15, 2) not null,
    DISTR_AMOUNT           DECIMAL(15, 2) not null,
    CUST_ID                INTEGER        not null,
    PAYMENT_TYPE           CHAR(1)        not null,
    ACCOUNT_NUMBER         CHAR(40)       not null,
    SWIFT_SHORT_BIC        CHAR(11)       not null,
    INSTRUMENT_JUSTIFIC    INTEGER        not null,
    TMSTAMP                TIMESTAMP(6)   not null,
    FK_HEADER_DEAL_NO      INTEGER        not null,
    FK_HEADER_DISTR_SN     INTEGER        not null,
    FK_CURRENCYID_CURRENCY INTEGER,
    constraint TRS_DISTR_PAY
        primary key (FK_HEADER_DEAL_NO, FK_HEADER_DISTR_SN, CUST_ID)
);

comment on table TRS_MM_DISTR_DTL is 'Holds distribution info per participant for each syndicated corporate loan payment.';

comment on column TRS_MM_DISTR_DTL.PAYMENT_TYPE is '1: PROFITS ACCOUNT2: SWIFT MESSAGE3: EFT MESSAGE';

