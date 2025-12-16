create table BANKEMPLOYEE
(
    ID                 CHAR(8),
    FKGD_HAS_AS_GRADE  INTEGER,
    FKGD_WORKS_IN_POSI INTEGER,
    TMSTAMP            TIMESTAMP(6),
    SIGNATURE_LEVEL    CHAR(1),
    EMPL_STATUS        CHAR(1),
    FKGH_WORKS_IN_POSI CHAR(5),
    FKGH_HAS_AS_GRADE  CHAR(5),
    CARD_ID            CHAR(8),
    STAFF_NO           CHAR(8),
    FATHER_NAME        CHAR(40),
    FIRST_NAME         VARCHAR(20),
    LAST_NAME          VARCHAR(20),
    SEX                CHAR(1),
    EMAIL              CHAR(40),
    WORK_PHONE         VARCHAR(20),
    MOBILE_PHONE       VARCHAR(20)
);

create unique index INDX_BANKEMPLOYEE
    on BANKEMPLOYEE (STAFF_NO);

create unique index IXU_BAN_001
    on BANKEMPLOYEE (ID);

