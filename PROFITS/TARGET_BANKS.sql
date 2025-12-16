create table TARGET_BANKS
(
    LFD                CHAR(10) not null
        constraint PK_LFD
            primary key,
    BIC                CHAR(11),
    HEADER_BIC         CHAR(11),
    ACCOUNT_HOLDER_BIC CHAR(11),
    BANK_NAME          CHAR(70),
    BANK_CITY          CHAR(35),
    NATIONAL_CODE      CHAR(15),
    MAIN_BIC_FLAG      CHAR(1),
    VALID_FROM         DATE,
    VALID_TO           DATE,
    PARTICIPATION_TYPE CHAR(2),
    SERVICE_FOR_TARGET CHAR(3),
    SERVICE_SCT        CHAR(1),
    SERVICE_SDD        CHAR(1),
    SERVICE_B2B        CHAR(1),
    OVERFLOW_FLAG      CHAR(1),
    TMSTAMP            TIMESTAMP(6),
    FK_BANK_ID         INTEGER,
    COUNTRY            CHAR(35)
);

create unique index IX_BIC
    on TARGET_BANKS (BIC);

