create table DIRECT_DEBIT_PAYS
(
    CNTR                           DECIMAL(15) not null
        constraint IXU_CP_063
            primary key,
    DEP_TUN_INT_SN                 SMALLINT,
    CRED_TUN_INT_SN                SMALLINT,
    PRIORITY                       INTEGER,
    FINAL_UNIT                     INTEGER,
    DEB_TRX_UNIT                   INTEGER,
    FK_DIRECT_DEBITFK0FAMILY_DETAI INTEGER,
    CRED_TRX_UNIT                  INTEGER,
    CUST_ID                        INTEGER,
    DEP_TRX_USR_SN                 INTEGER,
    CRED_TRX_USR_SN                INTEGER,
    FK0DIRECT_DEBITFK_FAMILY_DETAI DECIMAL(11),
    FK_DIRECT_DEBITFK_FAMILY_DETAI DECIMAL(11),
    AMOUNT_WAS_REQUESTED           DECIMAL(15, 2),
    AMOUNT_WAS_PAID                DECIMAL(15, 2),
    ACTUAL_PAY_DATE                DATE,
    DEB_TRX_DATE                   DATE,
    PAY_DATE                       DATE,
    TRANS_DATE                     DATE,
    CRED_TRX_DATE                  DATE,
    TMSTAMP                        TIMESTAMP(6),
    FK_DIRECT_DEBITTMSTAMP         TIMESTAMP(6),
    STATUS                         CHAR(1),
    ENTRY_STATUS                   CHAR(1),
    FK1DIRECT_DEBITFK_FAMILY_DETAI CHAR(5),
    DEP_TRX_USR                    CHAR(8),
    CRED_TRX_USR                   CHAR(8),
    INS_USR                        CHAR(8),
    FK2DIRECT_DEBITFK_FAMILY_DETAI CHAR(24),
    DESCRIPTION                    VARCHAR(100)
);

