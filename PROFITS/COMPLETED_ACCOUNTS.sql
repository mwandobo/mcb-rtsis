create table COMPLETED_ACCOUNTS
(
    ACCOUNT_NUMBER   DECIMAL(11) not null
        constraint IXU_DEP_120
            primary key,
    TRANSITION_INTER DECIMAL(15, 2),
    PROCESSED_DATE   DATE,
    ENTRY_STATUS     CHAR(1),
    FILE_NAME        CHAR(50),
    ERROR_DESC       CHAR(80)
);

