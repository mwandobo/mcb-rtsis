create table DIAS_ORD_DSCOMM
(
    SERVICE_CODE  CHAR(2) not null,
    ORDER_ORIGIN  CHAR(1) not null,
    TRANS_CODE    CHAR(3) not null,
    ID_COMMISSION INTEGER,
    constraint IXU_CP_088
        primary key (SERVICE_CODE, ORDER_ORIGIN, TRANS_CODE)
);

