create table BOND_RATE_TABLE
(
    RATE_TABLE_NUMBER DECIMAL(10) not null
        constraint IXU_DEP_116
            primary key,
    TMSTAMP           TIMESTAMP(6),
    ACTIVATION_DATE   DATE,
    RATE_TABLE_TYPE   CHAR(1),
    ENTRY_STATUS      CHAR(1),
    ACTIVATION_TIME   TIME
);

