create table PAYROLL_ACC_STATUS
(
    EMP_ID           INTEGER not null,
    ORG_ID           INTEGER not null,
    STAUS            SMALLINT,
    PAYROLL_COUNT    INTEGER,
    LAST_KNOWN_LIMIT DECIMAL(15, 2),
    TIMESTMP         TIMESTAMP(6),
    PROFITS_ACCOUNT  CHAR(40),
    constraint IXU_CP_100
        primary key (EMP_ID, ORG_ID)
);

