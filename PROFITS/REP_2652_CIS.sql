create table REP_2652_CIS
(
    CUST_ID         INTEGER not null,
    FIRST_NAME      CHAR(20),
    SURNAME         CHAR(70),
    FATHER_NAME     CHAR(20),
    AFM_NO          CHAR(20),
    MONITORE_UNIT   INTEGER,
    PROFFES_ID      INTEGER,
    TAX_OFFICE_NAME VARCHAR(20),
    LAWSHAPE        VARCHAR(40),
    CUST_ADDRESS    VARCHAR(100),
    ACTIVITY_SECTOR VARCHAR(40),
    DATE_OF_BIRTH   DATE,
    NATIONALITY     VARCHAR(40),
    ID_NO           CHAR(20),
    TELEPHONE       CHAR(15)
);

create unique index IXU_TBL_2652
    on REP_2652_CIS (CUST_ID);

