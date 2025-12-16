create table FRT_SWIFT_ADDRESS
(
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
    EXPIRY_DATE           DATE,
    SORT_CODE             CHAR(11),
    ENG_BANK_NAME         CHAR(70),
    SCT_PARTICIPATION     CHAR(1),
    URGENT_SCT            CHAR(1),
    SCT_INST              CHAR(1),
    CCP_PARTICIPATION     CHAR(1),
    CURRENT_SCT_INST      CHAR(1),
    CURRENT_MOBILE        CHAR(1),
    ACMT_PARTICIP         CHAR(1),
    TMSTAMP               TIMESTAMP(6),
    CITY                  CHAR(35),
    constraint PK_PARTIC_BANK
        primary key (PRODUCT, CSM_CODE, PARTICIPANT_BANK)
);

create unique index FRT_SWIFT_ADDRESS_IND1
    on FRT_SWIFT_ADDRESS (CSM_NAME);

