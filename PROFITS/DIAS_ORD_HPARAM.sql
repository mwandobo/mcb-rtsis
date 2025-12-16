create table DIAS_ORD_HPARAM
(
    ORDER_ORIGIN CHAR(1) not null,
    TRANS_CODE   CHAR(3) not null,
    constraint IXU_CP_062
        primary key (ORDER_ORIGIN, TRANS_CODE)
);

