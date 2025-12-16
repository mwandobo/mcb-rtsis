create table LNS_STANDING_PREM
(
    TRX_DATE          DATE         not null,
    ACCOUNT_NUMBER    CHAR(40)     not null,
    ACCOUNT_CD        SMALLINT,
    PRFT_SYSTEM       SMALLINT,
    ACC_UNIT          INTEGER,
    ACC_SN            INTEGER,
    ACC_TYPE          SMALLINT,
    ACC_CD            SMALLINT,
    DEP_ACC_NUMBER    DECIMAL(11)  not null,
    LOAN_ASKED_AMN    DECIMAL(15, 2),
    DEP_AVAILABLE_AMN DECIMAL(15, 2),
    TMSTAMP           TIMESTAMP(6) not null,
    constraint PK_STAD_PREM
        primary key (ACCOUNT_NUMBER, TRX_DATE, DEP_ACC_NUMBER, TMSTAMP)
);

