create table LOAN_ACCOUNT_SELECTION
(
    SCHEDULED_DATE        DATE     not null,
    PROGRAM_ID            CHAR(5)  not null,
    JOB_TYPE              CHAR(3)  not null,
    PRFT_SYSTEM           SMALLINT not null,
    ACCOUNT_NUMBER        CHAR(40) not null,
    ERROR_DESCRIPTION     VARCHAR(100),
    PROCESSED_TMSTAMP     TIMESTAMP(6),
    PROCESSED_STATUS      CHAR(1),
    TEXT_01               VARCHAR(100),
    TEXT_02               VARCHAR(100),
    INSTANCE_NO_A         CHAR(5),
    INSTANCE_NO_B         CHAR(5),
    DATE_01               DATE,
    DATE_02               DATE,
    LINKED_ACCOUNT_NUMBER CHAR(40),
    LINKED_ACCOUNT_SYSTEM SMALLINT,
    constraint I0000675
        primary key (ACCOUNT_NUMBER, PRFT_SYSTEM, JOB_TYPE, PROGRAM_ID, SCHEDULED_DATE)
);

