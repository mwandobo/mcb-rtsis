create table CMS_LIMIT
(
    CODE            CHAR(15) not null
        constraint PK_LIMIT0
            primary key,
    DESCRIPTION     CHAR(80),
    COMMENTS        VARCHAR(200),
    PROCESSING_CODE CHAR(10),
    ENTRY_STATUS    CHAR(1),
    TUN_DATE        DATE,
    TUN_UNIT        INTEGER,
    TUN_USR         CHAR(8),
    TUN_USR_SN      INTEGER,
    TUN_USR_INT_SN  SMALLINT,
    TMSTAMP         TIMESTAMP(6)
);

