create table LOAN_ACCNT_PROCESS
(
    RECORD_SN          DECIMAL(15),
    ACC_UNIT           INTEGER,
    ACC_TYPE           SMALLINT,
    ACC_SN             DECIMAL(15),
    PROJECT            SMALLINT,
    ACC_CD             SMALLINT,
    COMPOSITE_JUSTIFIC INTEGER,
    ACC_ID_CURRENCY    INTEGER,
    JUSTIFIC           INTEGER,
    DCD_RULE_ID        DECIMAL(12),
    AMOUNT_1           DECIMAL(15, 2),
    AMOUNT_2           DECIMAL(15, 2),
    START_TMSTAMP      TIMESTAMP(6),
    END_TMSTAMP        TIMESTAMP(6),
    PROCESS_DATE       DATE,
    VALUE_DATE         DATE,
    FORCED_FLG         CHAR(1),
    LOAN_STATUS        CHAR(1),
    PROCESS_FLG        CHAR(2),
    PROCESS_DESC       CHAR(50),
    ACCOUNT_SER_NUM    DECIMAL(11),
    CREATION_TMSTAMP   TIMESTAMP(6),
    CREATION_USR       CHAR(8),
    REQUEST_SN         SMALLINT,
    REQUEST_TYPE       CHAR(1),
    REQUEST_LOAN_STS   CHAR(1),
    ACCOUNT_NUMBER_1   CHAR(40),
    ACCOUNT_NUMBER_2   CHAR(40),
    ACCOUNT_NUMBER_3   CHAR(40),
    ACCOUNT_NUMBER_4   CHAR(40),
    ACCOUNT_NUMBER_5   CHAR(40),
    AMOUNT_3           DECIMAL(15, 2),
    AMOUNT_4           DECIMAL(15, 2),
    AMOUNT_5           DECIMAL(15, 2),
    JUSTIFIC_1         INTEGER,
    JUSTIFIC_2         INTEGER,
    JUSTIFIC_3         INTEGER,
    JUSTIFIC_4         INTEGER,
    JUSTIFIC_5         INTEGER,
    DATE_1             DATE,
    DATE_2             DATE,
    DATE_3             DATE,
    DATE_4             DATE,
    DATE_5             DATE,
    FLAG_1             CHAR(1),
    FLAG_2             CHAR(1),
    FLAG_3             CHAR(1),
    FLAG_4             CHAR(1),
    FLAG_5             CHAR(1),
    COMMENTS_1         CHAR(40),
    COMMENTS_2         CHAR(40),
    COMMENTS_3         CHAR(40),
    COMMENTS_4         CHAR(40),
    COMMENTS_5         CHAR(40),
    TRANSACT_1         INTEGER,
    TRANSACT_2         INTEGER,
    TRANSACT_3         INTEGER,
    TRANSACT_4         INTEGER,
    TRANSACT_5         INTEGER,
    CD_1               SMALLINT,
    CD_2               SMALLINT,
    CD_3               SMALLINT,
    CD_4               SMALLINT,
    CD_5               SMALLINT,
    NUMBER_1           DECIMAL(10),
    NUMBER_2           DECIMAL(10),
    NUMBER_3           DECIMAL(10),
    NUMBER_4           DECIMAL(10),
    NUMBER_5           DECIMAL(10)
);

create unique index IDX_LOAN_ACNT_JUSTIFIC
    on LOAN_ACCNT_PROCESS (JUSTIFIC, ACC_SN);

create unique index IXN_LOA_034
    on LOAN_ACCNT_PROCESS (PROCESS_FLG);

create unique index IXU_LOA_034
    on LOAN_ACCNT_PROCESS (RECORD_SN, ACC_UNIT, ACC_TYPE, ACC_SN);

create unique index SK_LOAN_ACCNT_PROCESS
    on LOAN_ACCNT_PROCESS (ACCOUNT_SER_NUM);

