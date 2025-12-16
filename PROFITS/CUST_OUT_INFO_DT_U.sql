create table CUST_OUT_INFO_DT_U
(
    FK_CUSTOMERCUST_ID INTEGER  not null,
    FK_CUST_OUT_INFHEA SMALLINT,
    FK_GENERIC_DETAFK  CHAR(5),
    FK_GENERIC_DETASER INTEGER,
    OUT_ACCOUNT        CHAR(21) not null,
    VALUE_1            CHAR(3),
    VALUE_2            DATE,
    VALUE_3            DECIMAL(15, 2),
    VALUE_4            DECIMAL(15, 2),
    VALUE_5            CHAR(20),
    constraint IXU_CIU_031
        primary key (OUT_ACCOUNT, FK_CUSTOMERCUST_ID)
);

