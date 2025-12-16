create table REMITT_INTERFACE
(
    ORDER_NUM       DECIMAL(10) not null
        constraint I0000060
            primary key,
    REF_KEY         CHAR(40)    not null,
    UNIT_IN         INTEGER     not null,
    SYSTEM          CHAR(10),
    USER_IN         CHAR(8),
    DATE_IN         DATE,
    TRANS_TYPE      CHAR(1),
    TRANS_CURR      INTEGER,
    TRANS_AMOUNT    DECIMAL(15, 2),
    ORDER_CURR      INTEGER,
    ORDER_AMOUNT    DECIMAL(15, 2),
    FX_RATE         DECIMAL(8, 4),
    STATUS          CHAR(1),
    FIRST_NAME      CHAR(20),
    SURNAME         CHAR(20),
    ADDRESS1        CHAR(40),
    ADDRESS3        CHAR(40),
    ADDRESS2        CHAR(40),
    PASS_NUMB       CHAR(20),
    COMMENTS        CHAR(70),
    TIMESTAMP       TIMESTAMP(6),
    TRX_UNIT        INTEGER,
    TRX_DATE        DATE,
    TRX_USR         CHAR(8),
    TRX_SN          INTEGER,
    TUN_INTERNAL_SN SMALLINT,
    CUST_ID_FROM    INTEGER,
    CUST_ID_TO      INTEGER
);

