create table AGREEMENT_TYPE
(
    FK_PRODUCTID_PRODU INTEGER,
    TMSTAMP            TIMESTAMP(6),
    ACC_EXPIRY         CHAR(1),
    ONE_ACCOUNT_FLG    CHAR(1),
    AGR_EXPIRY         CHAR(1),
    AGR_LIMIT_IND      CHAR(1)
);

create unique index IXU_AGR_004
    on AGREEMENT_TYPE (FK_PRODUCTID_PRODU);

