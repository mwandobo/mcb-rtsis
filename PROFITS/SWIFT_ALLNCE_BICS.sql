create table SWIFT_ALLNCE_BICS
(
    BIC                   CHAR(11),
    SWIFT_CONN_FLAG       CHAR(1),
    PREFIX                CHAR(3),
    VALUE_ADDED_1         CHAR(12),
    LOCATION              CHAR(105),
    ADDRESS_2             VARCHAR(35),
    VALUE_ADDED_2         VARCHAR(35),
    CITY                  VARCHAR(35),
    ADDRESS_3             VARCHAR(35),
    ADDRESS               VARCHAR(35),
    POB                   VARCHAR(35),
    SUB_INFOR             VARCHAR(52),
    BANK_DEPT             VARCHAR(70),
    COUNTRY               VARCHAR(70),
    COUNTRY_2             VARCHAR(77),
    ZIP_CODE_TOWN         VARCHAR(105),
    BANK_DESCR            VARCHAR(105),
    EXTRA_ADDR_INFO       VARCHAR(105),
    CENTRAL_BANK          CHAR(1),
    BRANCH_CODE           CHAR(3),
    INSTITUTION_NAME_1    CHAR(105),
    BRANCH_INFORMATION_1  CHAR(70),
    CITY_HEADING          CHAR(35),
    SUBTYPE_INDICATION    CHAR(4),
    VALUE_ADDED_SERVICES  CHAR(60),
    EXTRA_INFORMATION     CHAR(35),
    ADDRESS_4             CHAR(35),
    POB_LOCATION_1        CHAR(105),
    POB_COUNTRY_NAME_1    CHAR(70),
    RECORD_STATUS         CHAR(1),
    OPERLLY_ACTIVE_RECORD CHAR(1)
);

create unique index IXU_SWI_012
    on SWIFT_ALLNCE_BICS (BIC);

