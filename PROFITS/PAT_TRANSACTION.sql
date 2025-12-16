create table PAT_TRANSACTION
(
    ID             INTEGER  not null
        constraint PATTXPK1
            primary key,
    TXN_NAME       CHAR(20) not null,
    VARIATION      CHAR(40),
    TEST_CATEGORY  CHAR(10),
    RECORDING_TYPE SMALLINT,
    STATUS         CHAR(1)  not null,
    SUBSET_CODE    INTEGER
);

