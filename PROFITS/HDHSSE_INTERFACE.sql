create table HDHSSE_INTERFACE
(
    PROGRAM_ID      CHAR(5)      not null,
    ACH_SETTLE_DATE CHAR(8)      not null,
    CUTOFF_SN       SMALLINT     not null,
    IDENTIFIER      INTEGER      not null,
    TMSTAMP         TIMESTAMP(6) not null,
    ACH_BANK_CODE   CHAR(3)      not null,
    PAY_BRANCH      CHAR(4),
    PAY_ACCNO       CHAR(23),
    IBAN_CD         SMALLINT,
    IBAN_CNTRY      CHAR(2),
    IBAN_DIGITS     CHAR(4),
    CHQ_NO          CHAR(20),
    AMOUNT          CHAR(15),
    ACH_CURRENCY    CHAR(5)      not null,
    TRAN_DATE       CHAR(8),
    ISS_DATE        CHAR(8),
    BUY_BRANCH      CHAR(4),
    SETTLE_REF      CHAR(1),
    ACC_CODE        CHAR(2),
    TRAN_CODE       CHAR(8),
    DHSSE_REF       CHAR(7),
    FILENAME        CHAR(20),
    ENTRY_STATUS    CHAR(1),
    PROCESSING_ID   CHAR(10),
    constraint PK_HDHSSE_INTERF
        primary key (TMSTAMP, IDENTIFIER, CUTOFF_SN, ACH_SETTLE_DATE, PROGRAM_ID)
);

