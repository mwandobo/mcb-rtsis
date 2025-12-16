create table DIAS_ORD_DTIME
(
    ORDER_ORIGIN CHAR(1) not null,
    TRANS_CODE   CHAR(3) not null,
    TIME_TO      SMALLINT,
    TIME_FROM    SMALLINT,
    constraint IXU_CP_089
        primary key (ORDER_ORIGIN, TRANS_CODE)
);

