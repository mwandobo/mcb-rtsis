create table CUSTOMER_CREDIT_LINE
(
    CRLINE_AMOUNT                  DECIMAL(15, 2) not null,
    EXPIRY_DATE                    DATE           not null,
    UTILISED_AMOUNT                DECIMAL(15, 2) not null,
    ENTRY_STATUS                   CHAR(1)        not null,
    TMSTAMP                        CHAR(20)       not null,
    FK_CUSTOMERCUST_ID             INTEGER        not null,
    FK_CURRENCYID_CURRENCY         INTEGER        not null,
    FK_GENERIC_DETAFK_GENERIC_HEAD CHAR(5)        not null,
    FK_GENERIC_DETASERIAL_NUM      INTEGER        not null,
    FK_USRCODE                     CHAR(8),
    FK_UNITCODE                    INTEGER,
    constraint PKCREDIT
        primary key (FK_GENERIC_DETAFK_GENERIC_HEAD, FK_GENERIC_DETASERIAL_NUM, FK_CURRENCYID_CURRENCY,
                     FK_CUSTOMERCUST_ID, TMSTAMP)
);

