create table KAP_IDS
(
    KAP_ID      CHAR(20),
    KAP_ID_TYPE INTEGER,
    CUST_ID     INTEGER,
    TMSTAMP     TIMESTAMP(6)
);

create unique index IXU_KAP_001
    on KAP_IDS (KAP_ID, KAP_ID_TYPE);

