create table SUSPECT_CUST
(
    SUSPECT_CUST_ID         DECIMAL(10),
    ID_TYPE                 INTEGER,
    CUST_ID                 INTEGER,
    TMSTAMP                 TIMESTAMP(6),
    DATE_TO                 DATE,
    DATE_OF_BIRTH           DATE,
    DATE_FROM               DATE,
    ENTRY_STATUS            CHAR(1),
    CUST_TYPE               CHAR(1),
    ZIP_CODE                CHAR(10),
    COUNTRY                 CHAR(10),
    PHONE_NUM               CHAR(15),
    ID_NUM                  CHAR(20),
    FATHER_NAME             CHAR(20),
    FIRST_NAME              CHAR(20),
    AFM                     CHAR(20),
    CITY                    CHAR(30),
    ADDRESS                 CHAR(40),
    ALERT_AGENCY            CHAR(50),
    SURNAME                 CHAR(70),
    ALERT_REASON            CHAR(150),
    ALERT_REASON_2          CHAR(240),
    AGENCY_REFERENCE_NUMBER CHAR(8)
);

create unique index IXU_SUS_004
    on SUSPECT_CUST (SUSPECT_CUST_ID);

