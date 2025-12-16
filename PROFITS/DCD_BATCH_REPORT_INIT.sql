create table DCD_BATCH_REPORT_INIT
(
    BATCH_ID    CHAR(5) not null
        constraint IXU_DCD_049
            primary key,
    FK_DCD_REP  DECIMAL(12),
    FK_DCD_PROJ DECIMAL(12)
);

