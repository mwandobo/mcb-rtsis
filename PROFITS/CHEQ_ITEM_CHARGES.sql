create table CHEQ_ITEM_CHARGES
(
    ID_CHEQ_ITEM_CHARG INTEGER,
    TMSTAMP            TIMESTAMP(6),
    ENTRY_STATUS       CHAR(1),
    SHORT_DESCR        CHAR(5),
    DESCRIPTION        CHAR(40)
);

create unique index IXU_CHE_000
    on CHEQ_ITEM_CHARGES (ID_CHEQ_ITEM_CHARG);

