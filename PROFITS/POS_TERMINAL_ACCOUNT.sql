create table POS_TERMINAL_ACCOUNT
(
    TERMINAL_ID    CHAR(8) not null
        constraint PK_POS_TERMIN
            primary key,
    ACCOUNT_NUMBER CHAR(40),
    ACCOUNT_CD     SMALLINT,
    DEP_ACC_NUMBER DECIMAL(11),
    ENTRY_STATUS   CHAR(1)
);

