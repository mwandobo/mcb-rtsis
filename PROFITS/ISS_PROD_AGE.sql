create table ISS_PROD_AGE
(
    ID       INTEGER,
    AGE_FROM SMALLINT,
    AGE_TO   SMALLINT,
    PRODUCT  INTEGER,
    DESCR    CHAR(40)
);

create unique index IXU_ISS_013
    on ISS_PROD_AGE (ID);

