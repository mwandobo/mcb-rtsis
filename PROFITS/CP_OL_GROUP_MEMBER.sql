create table CP_OL_GROUP_MEMBER
(
    FK_CPGROUP_CUST_ID   INTEGER     not null,
    FK_CPGROUP_AGREEM_NO DECIMAL(10) not null,
    MEMBER_ID            DECIMAL(15) not null,
    GUI_CHARGES_PERC     DECIMAL(8, 4),
    GUI_COMMISS_PERC     DECIMAL(8, 4),
    GUI_COMMISSION       DECIMAL(15, 2),
    GUI_CHARGES          DECIMAL(15, 2),
    GUI_ORG_AMNT         DECIMAL(15, 2),
    TMSTAMP              TIMESTAMP(6),
    ENTRY_STATUS         CHAR(1),
    BEN_PASSPORT_TYPE    CHAR(1),
    BENEF_PASSPORT       CHAR(10),
    BENEF_ZIP_CODE       CHAR(10),
    BENEF_TELEPHONE      CHAR(15),
    BENEF_CITY           CHAR(15),
    KEY_FIELD_2          CHAR(80),
    KEY_FIELD_4          CHAR(30),
    KEY_FIELD_3          CHAR(30),
    KEY_FIELD_1          CHAR(80),
    REMARKS              CHAR(100),
    BENEF_ADDRESS2       VARCHAR(40),
    BENEF_ADDRESS1       VARCHAR(40),
    BENEF_COUNTRY        VARCHAR(40),
    BENEF_NAME           VARCHAR(40),
    constraint IXU_CP_115
        primary key (FK_CPGROUP_CUST_ID, FK_CPGROUP_AGREEM_NO, MEMBER_ID)
);

