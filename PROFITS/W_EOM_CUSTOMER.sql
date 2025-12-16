create table W_EOM_CUSTOMER
(
    EOM_DATE                DATE    not null,
    CUST_ID                 INTEGER not null,
    LOANS_COUNT             DECIMAL(10),
    LOANS_EXPOSURE_LCY      DECIMAL(15, 2),
    OVERDRAFTS_COUNT        DECIMAL(10),
    OVERDRAFTS_EXPOSURE_LCY DECIMAL(15, 2),
    LG_COUNT                DECIMAL(10),
    LG_EXPOSURE_LCY         DECIMAL(15, 2),
    LC_COUNT                DECIMAL(10),
    LC_EXPOSURE_LCY         DECIMAL(15, 2),
    constraint PK_W_EOM_CUSTOMER
        primary key (EOM_DATE, CUST_ID)
);

