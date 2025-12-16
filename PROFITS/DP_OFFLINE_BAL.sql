create table DP_OFFLINE_BAL
(
    ACCOUNT_NUMBER DECIMAL(11) not null,
    ENTRY_DATE     DATE        not null,
    ACC_CD         SMALLINT,
    TRX_SN         INTEGER,
    BALANCE        DECIMAL(15, 2),
    TRX_AMOUNT     DECIMAL(15, 2),
    TIMESTMP       DATE,
    STATUS         CHAR(1),
    TYPE           CHAR(1),
    ERR_DESC       CHAR(40),
    constraint IXU_DEP_135
        primary key (ACCOUNT_NUMBER, ENTRY_DATE)
);

