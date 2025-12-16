create table ISS_PROD_DOC_APPR
(
    ID_PROFILE CHAR(8),
    ID_DOC     INTEGER
);

create unique index IXU_ISS_009
    on ISS_PROD_DOC_APPR (ID_PROFILE, ID_DOC);

