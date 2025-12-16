create table COS_MEMB_CONTRIB
(
    SERVICE_PRODUCT INTEGER     not null,
    MEMBER_ID       DECIMAL(10) not null,
    INSURANCE_FEE   DECIMAL(15, 2),
    FUNERAL_FEE     DECIMAL(15, 2),
    CAPITAL_AMNT    DECIMAL(15, 2),
    SAYE_AMOUNT     DECIMAL(15, 2),
    BL_AMOUNT       DECIMAL(15, 2),
    constraint PK_COS_CONTR
        primary key (MEMBER_ID, SERVICE_PRODUCT)
);

