create table TMP_ADDITIONAL_TRX
(
    ENTRY_SER_NUM      SMALLINT,
    TRX_UNIT           INTEGER,
    UNIT               INTEGER,
    TRANS_SER_NUM      INTEGER,
    FK_DEPOSIT_ACCOACC DECIMAL(11),
    DC_AMOUNT          DECIMAL(15, 2),
    AMOUNT             DECIMAL(15, 2),
    DR_CR_FLAG         DECIMAL(15, 2),
    ID_CURRENCY        DECIMAL(15, 2),
    ENTRY_STATUS       DECIMAL(15, 2),
    RATE               DECIMAL(15, 2),
    INTERNAL_SN        DECIMAL(15, 2),
    DEP_ACCOUNT        DECIMAL(15, 2),
    ISO_CURRENCY       DECIMAL(15, 2),
    TC_CODE            DECIMAL(15, 2),
    ID_JUSTIFIC        DECIMAL(15, 2),
    TRX_DATE           DATE,
    TMSTAMP            DATE,
    DATE_REC           DATE,
    VALEUR_DATE        DATE,
    TRX_USR            CHAR(4),
    FILE_NAME          CHAR(10),
    COMMENTS           CHAR(17),
    GLG_ACCOUNT_ID     CHAR(18)
);

