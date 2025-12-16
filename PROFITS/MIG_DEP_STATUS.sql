create table MIG_DEP_STATUS
(
    ENTRY_STATUS      CHAR(1)     not null,
    PROCESS_DATE      DATE        not null,
    ACCOUNT_NUMBER    DECIMAL(11) not null,
    PROCESS_STATUS    CHAR(1),
    PREV_ENTRY_STATUS CHAR(1),
    PROCESS_COMMENT   CHAR(250),
    constraint IXU_MIG_034
        primary key (ENTRY_STATUS, PROCESS_DATE, ACCOUNT_NUMBER)
);

