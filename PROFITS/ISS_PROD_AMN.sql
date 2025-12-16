create table ISS_PROD_AMN
(
    ID       INTEGER,
    PRODUCT  INTEGER,
    AMN_FROM DECIMAL(15, 2),
    AMN_TO   DECIMAL(15, 2),
    DESCR    CHAR(40)
);

create unique index IXU_ISS_012
    on ISS_PROD_AMN (ID);

