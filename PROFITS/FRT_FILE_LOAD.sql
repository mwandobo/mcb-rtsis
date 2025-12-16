create table FRT_FILE_LOAD
(
    FILE_TIMESTAMP        CHAR(14) not null,
    RECORD_TYPE           CHAR(1),
    SYSTEM_CODE           CHAR(3),
    FILE_TYPE             CHAR(3),
    CSM_CODE              CHAR(11) not null,
    CSM_NAME              CHAR(35),
    PRODUCT               CHAR(8)  not null,
    PARTICIPANT_BANK      CHAR(11) not null,
    PARTICIPANT_BANK_CODE CHAR(3),
    BANK_NAME             CHAR(70),
    COUNTRY_OF_BANK       CHAR(2),
    SETTLEMENT_BANK       CHAR(11),
    LATEST_CUTOFF         SMALLINT,
    NATIONAL_CHAR_SET     CHAR(1),
    ACTIVATION_DATE       DATE,
    START_DATE            DATE,
    EXPIRY_DATE           DATE,
    STATUS_FLG            CHAR(1),
    ONLINE_MESSAGES       CHAR(1),
    ENG_BANK_NAME         CHAR(70),
    SCT_PARTICIPATION     CHAR(1),
    URGENT_SCT            CHAR(1),
    SCT_INST              CHAR(1),
    CCP_PARTICIPATION     CHAR(1),
    CURRENT_SCT_INST      CHAR(1),
    CURRENT_MOBILE        CHAR(1),
    ACMT_PARTICIP         CHAR(1),
    TMSTAMP               TIMESTAMP(6),
    constraint PK_FTR_FILE_LOAD
        primary key (PARTICIPANT_BANK, PRODUCT, CSM_CODE, FILE_TIMESTAMP)
);

