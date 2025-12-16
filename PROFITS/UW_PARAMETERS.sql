create table UW_PARAMETERS
(
    ID_NO             SMALLINT,
    DIV_PAYM_PROD_ID  INTEGER,
    CAP_INCR_PROD_ID  INTEGER,
    PUBLIC_UW_PROD_ID INTEGER,
    UW_PROD_ID        INTEGER,
    BLOCK_COEFF       DECIMAL(8, 4)
);

create unique index IXU_UW__004
    on UW_PARAMETERS (ID_NO);

