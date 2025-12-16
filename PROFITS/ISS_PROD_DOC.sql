create table ISS_PROD_DOC
(
    ID      INTEGER,
    PRODUCT INTEGER,
    DESCR   CHAR(40)
);

create unique index IXU_ISS_010
    on ISS_PROD_DOC (ID);

