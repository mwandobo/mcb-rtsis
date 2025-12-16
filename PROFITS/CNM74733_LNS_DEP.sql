create table CNM74733_LNS_DEP
(
    TIMESTAM          TIMESTAMP(6) not null,
    PR_ACCOUNT_NUMBER CHAR(40)     not null,
    ACCOUNT_NUMBER    DECIMAL(11)  not null,
    CCODE_ID          INTEGER      not null,
    ACC_OFFICER       CHAR(8)      not null,
    ACC_CODE          INTEGER      not null,
    PR_ACCOUNT_CD     SMALLINT,
    LACC_TYPE         SMALLINT,
    C_DIGIT           SMALLINT,
    UNITCODE          INTEGER,
    LACC_SN           INTEGER,
    ACC_LIMIT         DECIMAL(15, 2),
    ACCOUNT_BAL       DECIMAL(15, 2),
    AVAILABLE_BAL     DECIMAL(15, 2),
    constraint IXU_REP_039
        primary key (TIMESTAM, PR_ACCOUNT_NUMBER, ACCOUNT_NUMBER, CCODE_ID, ACC_OFFICER, ACC_CODE)
);

