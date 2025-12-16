create table FILE_TO_DB_HDR
(
    FILE_SN         DECIMAL(15) not null
        constraint IXU_CP_094
            primary key,
    TRX_INTERNAL_SN SMALLINT,
    TRX_UNIT        INTEGER,
    PRODUCT_ID      INTEGER,
    JUSTIF_ID       INTEGER,
    TRANS_ID        INTEGER,
    CUST_ID         INTEGER,
    TRX_SN          INTEGER,
    TRX_DATE        DATE,
    TIMESTMP        TIMESTAMP(6),
    TRX_USR         CHAR(8),
    PROFITS_ACCOUNT VARCHAR(40),
    FILE_EXTENSION  VARCHAR(4),
    FILE_NAME       VARCHAR(40),
    FILE_LENGTH     INTEGER,
    FILE_BLOB       BLOB(1048576),
    FILE_PATH_NAME  VARCHAR(255),
    PROFITS_SYSTEM  SMALLINT
);

