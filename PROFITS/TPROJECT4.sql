create table TPROJECT4
(
    PERSONAL_NUMBER     VARCHAR(50) not null
        constraint IXU_PRD_027
            primary key,
    OPERATOR_ID         SMALLINT,
    YEAR_MONTH          SMALLINT,
    DEF_AMMOUNT         DECIMAL(8, 2),
    ADD_AMMOUNT         DECIMAL(8, 2),
    TOTAL_AMMOUNT       DECIMAL(8, 2),
    PEOPLE_ID           DECIMAL(10),
    ID                  DECIMAL(10),
    OWNER_ID            DECIMAL(10),
    OWNER_ID1           DECIMAL(10),
    GRAND_TOTAL_AMOUNT  DECIMAL(15, 2),
    DATE1               DATE,
    DOB                 DATE,
    CAN_DELL            CHAR(1),
    STATUS              CHAR(1),
    NCITY               VARCHAR(50),
    DOCUMENT_NAME       VARCHAR(50),
    NSTREET             VARCHAR(50),
    FLAT_STATUS         VARCHAR(50),
    ORG_NAME            VARCHAR(50),
    MUNI                VARCHAR(50),
    MONTH_COUNT         VARCHAR(50),
    FT_NAME             VARCHAR(50),
    FR_NAME             VARCHAR(50),
    LS_NAME             VARCHAR(50),
    FAMILY_NUMBER       VARCHAR(50),
    NCITY_CODE          VARCHAR(50),
    IDP_PERSONAL_NUMBER VARCHAR(50),
    NVILAGE             VARCHAR(50)
);

