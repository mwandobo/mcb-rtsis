create table SQL_EXECUTE_LOG
(
    MULTI_ROW           DECIMAL(1)   not null,
    TIMESTMP            TIMESTAMP(6) not null,
    INTERNAL_SN         DECIMAL(15)  not null,
    SHEET               DECIMAL(5)   not null,
    DATA_SQL            VARCHAR(4000),
    DATA_SQL_SUBSTITUTE VARCHAR(4000),
    SQL_CRITERIA        VARCHAR(4000),
    RESULT0             VARCHAR(4000),
    FK_DCD_PROJECT      DECIMAL(12)  not null,
    FK_DCD_REPORT       DECIMAL(12)  not null,
    FK_DCD_INFO         DECIMAL(15)  not null,
    constraint PK_SQL_EXECUTE_LOG
        primary key (FK_DCD_PROJECT, FK_DCD_REPORT, FK_DCD_INFO, TIMESTMP)
);

