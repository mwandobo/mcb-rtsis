create table KAP_INTERFACE
(
    IDENTIFIER      INTEGER not null
        constraint IXU_DEP_137
            primary key,
    KAP_DATE        DATE,
    TMSTAMP         DATE,
    STATUS_REF      CHAR(1),
    IS_COMPANY      CHAR(3),
    COUNTRY_CODE    CHAR(3),
    ISSUE_CODE      CHAR(3),
    PASSPORT_NO     CHAR(10),
    DEC_REF         CHAR(10),
    LISTING_DATE    CHAR(10),
    CRITERIA_VALUE  CHAR(10),
    COUNTER         CHAR(10),
    COMPANY_TYPE    CHAR(15),
    REGISTRATION_NO CHAR(15),
    ID_NUMBER       CHAR(15),
    STATUS          CHAR(15),
    COUNTRY         CHAR(20),
    DEC_TYPE        CHAR(20),
    ISSUE_COUNTRY   CHAR(20),
    CRITERIA_FIELD  CHAR(30),
    DEC_TEXT        CHAR(40),
    CUSTOMER_NAME   CHAR(40),
    DEC_NOTES       CHAR(40)
);

