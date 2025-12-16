create table DP_INTER_RATE_ANAL
(
    PRODUCT_ID    INTEGER not null,
    CUR_SH_DESC   CHAR(5) not null,
    SN            INTEGER not null,
    DR_AMOUNT     DECIMAL(15, 2),
    DR_AMOUNT_DOM DECIMAL(15, 2),
    CR_AMOUNT     DECIMAL(15, 2),
    CR_AMOUNT_DOM DECIMAL(15, 2),
    constraint IXU_DEP_171
        primary key (PRODUCT_ID, CUR_SH_DESC, SN)
);

