create table LNS_STANDING_ORDER
(
    ACCOUNT_NUMBER    CHAR(40)     not null,
    TRX_DATE          DATE         not null,
    ACCOUNT_CD        SMALLINT,
    ACC_TYPE          SMALLINT,
    PRFT_SYSTEM       SMALLINT,
    ACC_CD            SMALLINT,
    ACC_UNIT          INTEGER,
    ACC_SN            INTEGER,
    DEP_ACC_NUMBER    DECIMAL(11)  not null,
    DEP_AVAILABLE_AMN DECIMAL(15, 2),
    LOAN_ASKED_AMN    DECIMAL(15, 2),
    LG_PROCESS_FLG    CHAR(1),
    TMSTAMP           TIMESTAMP(6) not null,
    SO_ACCOUNT_NUMBER CHAR(40),
    SO_PRFT_SYSTEM    SMALLINT,
    constraint IXU_LNS_022
        primary key (ACCOUNT_NUMBER, TRX_DATE, DEP_ACC_NUMBER, TMSTAMP)
);

