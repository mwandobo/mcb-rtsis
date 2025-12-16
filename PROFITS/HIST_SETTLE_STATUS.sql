create table HIST_SETTLE_STATUS
(
    ACCOUNT_NO         CHAR(20) not null
        constraint IXU_DEF_088
            primary key,
    ENTRY_STATUS       SMALLINT,
    TOTAL_BENEFICIARIE SMALLINT,
    FILE_NAME          CHAR(50)
);

