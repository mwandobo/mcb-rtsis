create table SUBS_KYA_REL
(
    PRODUCT_ID INTEGER,
    LOAN_PURP  INTEGER,
    KYA        SMALLINT
);

create unique index IXU_SUB_000
    on SUBS_KYA_REL (PRODUCT_ID, LOAN_PURP);

