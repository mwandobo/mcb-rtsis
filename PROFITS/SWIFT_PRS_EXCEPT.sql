create table SWIFT_PRS_EXCEPT
(
    PRFT_REF_NO    CHAR(16) not null,
    EXCEPTION_CODE CHAR(10) not null,
    SN             SMALLINT not null,
    EXCEPTION_TAG  CHAR(10),
    ACCEPT_REJECT  CHAR(2),
    VERIFY_USER_1  CHAR(8),
    VERIFY_USER_2  CHAR(8),
    ACCOUNT_NUMBER CHAR(40),
    ENTRY_STATUS   CHAR(1),
    TMSTAMP        TIMESTAMP(6),
    constraint PK_STP_EXEPT
        primary key (PRFT_REF_NO, EXCEPTION_CODE, SN)
);

