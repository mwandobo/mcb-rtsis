create table PROD_UNIT_GRP_HD
(
    PUG_ID             DECIMAL(10),
    LAST_UPDATE_DATE   TIMESTAMP(6),
    ENTRY_STATUS       CHAR(1),
    PENDING_RENEWAL_PS CHAR(1),
    LAST_UPDATE_USR    CHAR(8),
    DESCRIPTION        CHAR(40)
);

create unique index IXU_PRO_025
    on PROD_UNIT_GRP_HD (PUG_ID);

