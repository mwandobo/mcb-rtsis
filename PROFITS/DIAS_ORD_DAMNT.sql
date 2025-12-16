create table DIAS_ORD_DAMNT
(
    ORDER_ORIGIN CHAR(1) not null,
    TRANS_CODE   CHAR(3) not null,
    AMNT_TO      DECIMAL(15),
    AMNT_FROM    DECIMAL(15),
    constraint IXU_CP_122
        primary key (ORDER_ORIGIN, TRANS_CODE)
);

