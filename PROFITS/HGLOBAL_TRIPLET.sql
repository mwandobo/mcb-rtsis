create table HGLOBAL_TRIPLET
(
    PTJ_VALIDITY_DATE DATE    not null,
    PRODUCT_ID        INTEGER not null,
    TRANSACTION_ID    INTEGER not null,
    JUSTIFICATION_ID  INTEGER not null,
    PRD_VALIDITY_DATE DATE    not null,
    TAG_SET_CODE      CHAR(20),
    USED_FLG          CHAR(1),
    constraint PK_HGLOBAL
        primary key (PRD_VALIDITY_DATE, JUSTIFICATION_ID, TRANSACTION_ID, PRODUCT_ID, PTJ_VALIDITY_DATE)
);

