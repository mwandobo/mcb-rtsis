create table TEMP_CUSTOMER
(
    REFERENCE_NO       CHAR(15),
    C_DIGIT            SMALLINT,
    PROFCAT            INTEGER,
    PROFSTS            INTEGER,
    UNIT_CODE          INTEGER,
    PROFID             INTEGER,
    CUST_ID            INTEGER,
    BIRTH_DATE         DATE,
    MEMBER_SINCE       DATE,
    TMSTAMP            DATE,
    PERS_ID_ISSUE_DATE DATE,
    STATEMENT_ADDRESS  CHAR(1),
    SEX                CHAR(1),
    CUST_TYPE          CHAR(1),
    EMPL_POST_CODE     CHAR(10),
    HOME_POST_CODE     CHAR(10),
    EMPL_PHONE_NO      CHAR(15),
    MOBILE_PHONE_NO    CHAR(15),
    HOME_PHONE_NO      CHAR(15),
    PERS_ID_NO         CHAR(20),
    PERS_FATHER        CHAR(20),
    AFM                CHAR(20),
    PASSPORT_NUMBER    CHAR(20),
    NAME               CHAR(20),
    EMPL_TOWN          CHAR(30),
    HOME_TOWN          CHAR(30),
    PERS_ISSUNG_AUTHOR CHAR(30),
    HOME_ADDRESS       CHAR(40),
    EMPL_ADDRESS       CHAR(40),
    SURNAME            CHAR(70),
    PERS_LATIN_NAME    CHAR(90)
);

create unique index IXU_TEM_027
    on TEMP_CUSTOMER (REFERENCE_NO);

