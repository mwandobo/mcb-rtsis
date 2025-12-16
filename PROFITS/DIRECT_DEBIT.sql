create table DIRECT_DEBIT
(
    FK_FAMILY_DETAIFK_PROVIDER_ID  DECIMAL(11)  not null,
    FK_FAMILY_DETAIFK_FAMILY_CODE  DECIMAL(11)  not null,
    FK_FAMILY_DETAIFK_GENERIC_DETA CHAR(5)      not null,
    FK0FAMILY_DETAIFK_GENERIC_DETA INTEGER      not null,
    FK_FAMILY_DETAIACCOUNT_NO      CHAR(24)     not null,
    TMSTAMP                        TIMESTAMP(6) not null,
    PRIORITY                       INTEGER,
    FREQUENCY_SHIFT                INTEGER,
    MONTHS_DAYS                    DECIMAL(10),
    AMOUNT1                        DECIMAL(15, 2),
    AMOUNT2                        DECIMAL(15, 2),
    START_DATE                     DATE,
    END_DATE                       DATE,
    FINAL_DATE                     DATE,
    FREQUENCY                      CHAR(1),
    AMOUNT_TYPE                    CHAR(1),
    ENTRY_STATUS                   CHAR(1),
    constraint IXU_CP_090
        primary key (FK_FAMILY_DETAIFK_PROVIDER_ID, FK_FAMILY_DETAIFK_FAMILY_CODE, FK_FAMILY_DETAIFK_GENERIC_DETA,
                     FK0FAMILY_DETAIFK_GENERIC_DETA, FK_FAMILY_DETAIACCOUNT_NO, TMSTAMP)
);

