create table ISS_PROD_REJ
(
    ID      INTEGER,
    REJ_NUM SMALLINT,
    PRODUCT INTEGER,
    ID_APPR INTEGER,
    ID_DOC  INTEGER
);

create unique index IXU_ISS_007
    on ISS_PROD_REJ (ID);

