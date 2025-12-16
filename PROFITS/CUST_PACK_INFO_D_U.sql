create table CUST_PACK_INFO_D_U
(
    CUST_ID            INTEGER not null,
    SN                 INTEGER not null,
    ID_PACKAGE         INTEGER not null,
    ID_PRODUCT         INTEGER not null,
    PACKAGE_USAGE      SMALLINT,
    INSERTION_DT       DATE,
    MODIFICATION_DT    DATE,
    MODIFICATION_USER  CHAR(8),
    ACCOUNT_STATUS_FLG CHAR(1),
    PROFITS_ACC_FLG    CHAR(1),
    ACC_UNIT_CODE      INTEGER,
    ACC_TYPE           SMALLINT,
    ACCOUNT_NUMBER     CHAR(40),
    ACCOUNT_CD         SMALLINT,
    constraint IXU_CIU_032
        primary key (ID_PRODUCT, ID_PACKAGE, SN, CUST_ID)
);

