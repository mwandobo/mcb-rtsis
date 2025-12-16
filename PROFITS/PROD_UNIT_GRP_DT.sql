create table PROD_UNIT_GRP_DT
(
    MEMBER_TYPE CHAR(1),
    MEMBER_ID   INTEGER,
    PUG_ID      DECIMAL(10)
);

create unique index IXU_PRO_024
    on PROD_UNIT_GRP_DT (MEMBER_TYPE, MEMBER_ID, PUG_ID);

