create table SMP_BLOB_
(
    OWNER VARCHAR(32),
    ID    VARCHAR(125),
    SUBID VARCHAR(125),
    LEN   DECIMAL(15, 2),
    DATA  LONG VARCHAR(32700)
);

create unique index REPOSITORY_BLOB_ID
    on SMP_BLOB_ (ID, SUBID);

