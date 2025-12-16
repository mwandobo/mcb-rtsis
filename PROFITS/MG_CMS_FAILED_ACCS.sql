create table MG_CMS_FAILED_ACCS
(
    FILE_NAME      CHAR(50) not null,
    SN             INTEGER  not null,
    SEQUENCE_NO    CHAR(6)  not null,
    PAN            CHAR(19),
    ACCOUNT_NUMBER CHAR(28) not null,
    constraint PK_MG_CMS_FAILED_ACCS
        primary key (ACCOUNT_NUMBER, SEQUENCE_NO, SN, FILE_NAME)
);

