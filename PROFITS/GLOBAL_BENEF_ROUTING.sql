create table GLOBAL_BENEF_ROUTING
(
    SN                      DECIMAL(10) not null
        constraint PK_GLOBAL_BR
            primary key,
    TO_BENEF_NAME           VARCHAR(100),
    TO_BENEF_ADDRESS_1      VARCHAR(80),
    TO_BENEF_ADDRESS_2      VARCHAR(80),
    TO_IBAN_OR_ACCOUNT      CHAR(37),
    INDICATOR               CHAR(1),
    FROM_CUST_ID            INTEGER,
    FROM_PRFT_SYSTEM        SMALLINT,
    FROM_ACCOUNT_NUMBER     CHAR(40),
    OVERRIDE_CUSTOMER_ROUTE CHAR(1),
    ENTRY_COMMENTS          VARCHAR(100),
    CREATE_UNIT             INTEGER,
    CREATE_DATE             DATE,
    CREATE_USR              CHAR(8),
    CREATE_TMSTAMP          TIMESTAMP(6),
    UPDATE_UNIT             INTEGER,
    UPDATE_DATE             DATE,
    UPDATE_USR              CHAR(8),
    UPDATE_TMSTAMP          TIMESTAMP(6),
    ENTRY_STATUS            CHAR(1),
    FK_GH_ROUT3             CHAR(5),
    FK_GD_ROUT3             INTEGER,
    FK_GH_CNTRY             CHAR(5),
    FK_GD_CNTRY             INTEGER
);

