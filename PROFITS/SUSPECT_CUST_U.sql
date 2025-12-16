create table SUSPECT_CUST_U
(
    SUSPECT_CUST_ID DECIMAL(10) not null
        constraint IXU_CIU_057
            primary key,
    SURNAME         CHAR(70),
    FIRST_NAME      CHAR(20),
    FATHER_NAME     CHAR(20),
    CUST_TYPE       CHAR(1),
    DATE_OF_BIRTH   DATE,
    ID_TYPE         INTEGER,
    ID_NUM          CHAR(20),
    AFM             CHAR(20),
    ADDRESS         CHAR(40),
    ZIP_CODE        CHAR(10),
    CITY            CHAR(30),
    COUNTRY         CHAR(10),
    PHONE_NUM       CHAR(15),
    CUST_ID         INTEGER,
    DATE_FROM       DATE,
    DATE_TO         DATE,
    ALERT_AGENCY    CHAR(50),
    ALERT_REASON    CHAR(150),
    ENTRY_STATUS    CHAR(1),
    TMSTAMP         TIMESTAMP(6),
    ALERT_REASON_2  CHAR(240)
);

