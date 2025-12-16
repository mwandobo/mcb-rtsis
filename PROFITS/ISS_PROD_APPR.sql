create table ISS_PROD_APPR
(
    ID      INTEGER,
    PRODUCT INTEGER,
    ID_AGE  INTEGER,
    ID_AMN  INTEGER,
    ID_DOC  INTEGER,
    DESCR   CHAR(40)
);

create unique index IXU_ISS_011
    on ISS_PROD_APPR (ID);

