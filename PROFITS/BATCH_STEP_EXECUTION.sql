create table BATCH_STEP_EXECUTION
(
    JOB_EXECUTION_ID      CHAR(20),
    EXIT_CODE             INTEGER,
    EXIT_MESSAGE          VARCHAR(4000),
    CREATED_TIME          TIMESTAMP(6) default CURRENT TIMESTAMP,
    CREATED_BY_USER       VARCHAR(30)  default 'USER',
    CREATED_BY_OS_USER    VARCHAR(60)  default 'db2 n/a ?',
    CREATED_BY_IP_ADDRESS VARCHAR(30)  default 'db2 n/a ?',
    STEP_NAME             VARCHAR(100)
);

