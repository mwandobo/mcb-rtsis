create table ISSU_PROD_DET_SRC
(
    ID         INTEGER,
    FIELD_NAME CHAR(40)
);

create unique index IXU_ISS_003
    on ISSU_PROD_DET_SRC (ID);

