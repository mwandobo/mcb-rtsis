create table DIAS_PRODUCT_INFO
(
    SN            SMALLINT not null,
    DIAS_PRODUCT  CHAR(5)  not null,
    CR_DB_IND     CHAR(1),
    INFO_IND      CHAR(1),
    ENTRY_STATUS  CHAR(1),
    TARGET_AMOUNT CHAR(40),
    INFORMATION2  CHAR(40),
    INFORMATION   VARCHAR(40),
    constraint IXU_CP_123
        primary key (SN, DIAS_PRODUCT)
);

