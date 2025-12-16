create table TMP_CUSTREL
(
    C_DIGIT                SMALLINT,
    PROFESSION_CODE        INTEGER,
    FKCUST_HAS_AS_FIRS     INTEGER,
    CUST_ID                INTEGER,
    FKCUST_HAS_AS_SECO     INTEGER,
    MAIL_BOX               CHAR(5),
    ZIP_CODE               CHAR(10),
    RELATIONSHIP_TYPE      CHAR(12),
    ID_NO                  CHAR(20),
    AFM_NO                 CHAR(20),
    FIRST_NAME             CHAR(20),
    OTHER_RELATIONSHIP     CHAR(30),
    RELATIONSHIP           CHAR(30),
    CITY                   CHAR(30),
    SURNAME                CHAR(70),
    CUST_TYPE              VARCHAR(13),
    REGION                 VARCHAR(20),
    ADDRESS_TYPE           VARCHAR(20),
    COUNTRY                VARCHAR(40),
    PROFESSION_DESCRIPTION VARCHAR(40),
    ID_TYPE                VARCHAR(40),
    ADDRESS_1              VARCHAR(40),
    ADDRESS_2              VARCHAR(40)
);

create unique index IX_CUST_SECO
    on TMP_CUSTREL (FKCUST_HAS_AS_SECO);

