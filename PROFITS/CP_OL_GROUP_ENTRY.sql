create table CP_OL_GROUP_ENTRY
(
    TRX_UNIT          INTEGER,
    CPGROUP_CUST_ID   INTEGER,
    TRX_USR_SN        INTEGER,
    CPGROUP_AGREEM_NO DECIMAL(10),
    DEP_ACC_NUMBER    DECIMAL(11),
    REVERSED_ENTRY_NO DECIMAL(15),
    TRX_AMNT          DECIMAL(15, 2),
    ENTRY_NO          DECIMAL(15),
    TRX_DATE          DATE,
    VALEUR_DATE       DATE,
    TMSTAMP           TIMESTAMP(6),
    ENTRY_STATUS      CHAR(1),
    TRX_USR           CHAR(8),
    COMMENTS          CHAR(40)
);

create unique index SYS_C00116043
    on CP_OL_GROUP_ENTRY (ENTRY_NO);

