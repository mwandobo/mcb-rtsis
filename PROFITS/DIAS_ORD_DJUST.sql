create table DIAS_ORD_DJUST
(
    DETAIL_OF_CHARGE CHAR(3) not null,
    ORDER_ORIGIN     CHAR(1) not null,
    TRANS_CODE       CHAR(3) not null,
    ID_JUSTIFIC      INTEGER,
    constraint IXU_CP_061
        primary key (DETAIL_OF_CHARGE, ORDER_ORIGIN, TRANS_CODE)
);

