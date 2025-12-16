create table COLLECTION_CONNECT
(
    FK_COLLECTION_ASN INTEGER,
    TMSTAMP           TIMESTAMP(6),
    LNUM_FROM         SMALLINT,
    LNUM_TO           SMALLINT,
    BUCKET_TO         SMALLINT,
    BUCKET_FROM       SMALLINT,
    CODE_TO           INTEGER,
    PURP_FROM         INTEGER,
    PURP_TO           INTEGER,
    CODE_FROM         INTEGER,
    FINSORT_TO        INTEGER,
    FINSORT_FROM      INTEGER,
    PRODUCT_TO        INTEGER,
    PRODUCT_FROM      INTEGER,
    UNIT_TO           INTEGER,
    UNIT_FROM         INTEGER,
    SN_FROM           INTEGER,
    SN_TO             INTEGER,
    AMOUNT_FROM       DECIMAL(15, 2),
    AMOUNT_TO         DECIMAL(15, 2),
    LAST_UPDATE       TIMESTAMP(6),
    ENTRY_STATUS      CHAR(1),
    LOAN_STATUS       CHAR(1),
    LAST_USER         CHAR(8)
);

create unique index IXP_COL_001
    on COLLECTION_CONNECT (FK_COLLECTION_ASN, TMSTAMP);

