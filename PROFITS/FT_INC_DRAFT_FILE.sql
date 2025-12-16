create table FT_INC_DRAFT_FILE
(
    FILE_NAME          VARCHAR(50) not null,
    ACCOUNT_NO         CHAR(40)    not null,
    SERIAL_NO          DECIMAL(15) not null,
    ISSUE_RECEIPT_DATE DATE,
    BANK_DRAFT_NO      CHAR(10),
    BENEF_NAME         CHAR(80),
    DRAFT_AMOUNT       DECIMAL(15, 2),
    R_PROCESS_DATE     DATE,
    R_STATUS_DESC      CHAR(80),
    R_CREATE_DATE      DATE        not null,
    POSTED_UNIT        INTEGER     not null,
    POSTED_USR         CHAR(8)     not null,
    POSTED_SN          INTEGER,
    POSTED_ISN         SMALLINT,
    FILE_HASH          VARCHAR(50) not null,
    GROUP_NUMBER       DECIMAL(10) not null,
    TMSTAMP            TIMESTAMP(6),
    R_PROCESS_ACTION   CHAR(1)     not null,
    R_STATUS           SMALLINT    not null,
    constraint PK_FT_FILE_ID
        primary key (GROUP_NUMBER, FILE_HASH, SERIAL_NO, FILE_NAME)
);

create unique index SK_FT_FILE_ID
    on FT_INC_DRAFT_FILE (R_PROCESS_ACTION, ACCOUNT_NO, SERIAL_NO, R_STATUS);

create unique index SK_FT_FILE_ID1
    on FT_INC_DRAFT_FILE (FILE_NAME, FILE_HASH, R_STATUS);

