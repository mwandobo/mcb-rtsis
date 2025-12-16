create table CIE_AGGR_CUST_CHAN
(
    FK_CUSTOMER   INTEGER     not null,
    FK_PROFILE    SMALLINT    not null,
    FK_AGREEMENT  DECIMAL(10) not null,
    NICKNAME      CHAR(16)    not null,
    SEQ_ORDER_NBR SMALLINT,
    PREFF_FLD_4   VARCHAR(256),
    PREFF_FLD_2   VARCHAR(256),
    PREFF_FLD_1   VARCHAR(256),
    PREFF_FLD_3   VARCHAR(256),
    constraint IXU_DEF_112
        primary key (FK_CUSTOMER, FK_PROFILE, FK_AGREEMENT, NICKNAME)
);

