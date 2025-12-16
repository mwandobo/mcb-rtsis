create table AGR_MNT_BENEF_LOAD
(
    ACCOUNT_NO        CHAR(40) not null,
    BENEFICIARY_SN    SMALLINT not null,
    PRFT_CUST_C_DIGIT SMALLINT,
    PRFT_CUST_ID      INTEGER,
    BENEFICIARY_TYPE  CHAR(1),
    constraint IXU_MIG_036
        primary key (ACCOUNT_NO, BENEFICIARY_SN)
);

