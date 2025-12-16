create table CUST_VEHICLE_DATA
(
    VEHICLE_VALUE           DECIMAL(15, 2),
    REGISTRATION_NO         CHAR(15) not null,
    BRAND                   CHAR(40),
    MODEL                   CHAR(40),
    POWER_CC                CHAR(40),
    PERCENTAGE              SMALLINT,
    AMOUNT_IN_LC            DECIMAL(15, 2),
    TIMESTMP                TIMESTAMP(6),
    FK_CUST_CUSTOMERCUST_ID INTEGER  not null,
    FK_CUST_YEAR            SMALLINT not null,
    FK_GENERIC_DETAFK       CHAR(5)  not null,
    FK_GENERIC_DETASER      INTEGER  not null,
    FK0GENERIC_DETAFK       CHAR(5)  not null,
    FK0GENERIC_DETASER      INTEGER  not null,
    FK_CURRENCYID_CURR      INTEGER  not null,
    constraint PK_CUST_VEHICLE_TYPE
        primary key (FK_CURRENCYID_CURR, FK_GENERIC_DETAFK, FK_GENERIC_DETASER, FK0GENERIC_DETAFK, FK0GENERIC_DETASER,
                     FK_CUST_CUSTOMERCUST_ID, FK_CUST_YEAR, REGISTRATION_NO)
);

create unique index I0000677
    on CUST_VEHICLE_DATA (FK0GENERIC_DETAFK, FK0GENERIC_DETASER);

create unique index I0000679
    on CUST_VEHICLE_DATA (FK_CURRENCYID_CURR);

