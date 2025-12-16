create table DIAS_ORD_DCOMM
(
    SERVICE_CODE     CHAR(2) not null,
    TRANS_CODE       CHAR(3) not null,
    DETAIL_OF_CHARGE CHAR(3) not null,
    ORDER_ORIGIN     CHAR(1) not null,
    BANK_ID          INTEGER not null,
    ID_COMMISSION    INTEGER not null,
    ID_COMMISSI_OWN  INTEGER,
    constraint IXU_CP_087
        primary key (SERVICE_CODE, TRANS_CODE, DETAIL_OF_CHARGE, ORDER_ORIGIN, BANK_ID, ID_COMMISSION)
);

