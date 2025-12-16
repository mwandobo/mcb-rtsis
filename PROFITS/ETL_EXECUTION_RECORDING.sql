create table ETL_EXECUTION_RECORDING
(
    EXECUTION_SN DECIMAL(15) not null
        constraint PK_TRA_EXECUTION
            primary key,
    EXEC_TMSTAMP TIMESTAMP(6)
);

