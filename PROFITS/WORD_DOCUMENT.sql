create table WORD_DOCUMENT
(
    ACCOUNT_NUMBER   CHAR(40) not null,
    PRFT_SYSTEM      SMALLINT not null,
    SERIAL_NO        SMALLINT not null,
    CODE             INTEGER,
    RULE_ID          DECIMAL(12),
    LG_AMOUNT        DECIMAL(15),
    CR_DR_AMOUNT     DECIMAL(15),
    FINAL_DATE       DATE,
    CREATION_DATE    DATE,
    LAST_UPDATE_DT   DATE,
    LOCK_UNLOCK_FLG  CHAR(1),
    DOC_STATUS       CHAR(1),
    REFERENCE_NUMBER CHAR(10),
    FILE_NAME        CHAR(24),
    COMMENTS         CHAR(40),
    constraint IXU_WOR_001
        primary key (ACCOUNT_NUMBER, PRFT_SYSTEM, SERIAL_NO)
);

