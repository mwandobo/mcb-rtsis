create table AML_BLACK_LIST
(
    CUST_ID      INTEGER not null
        constraint IXU_AML_003
            primary key,
    BIRTHDATE    DATE,
    CHANGE_DATE  DATE,
    PROC_FLAG    CHAR(1),
    UPD_FLAG     CHAR(1),
    LASTNAME     CHAR(32),
    NAME_AFF     CHAR(32),
    FIRSTNAME    CHAR(32),
    SOURCE_OF_IN CHAR(32),
    ORGANIZATION CHAR(32),
    BIRTHPLACE   CHAR(32),
    PASSNO       CHAR(32),
    ADDRESS      CHAR(32),
    CITY_COUNTRY CHAR(32),
    NAME         CHAR(64),
    ALIAS        CHAR(64),
    REMARK       CHAR(254)
);

