create table DCD_BATCH_REPORT_PARAMS
(
    FK_DCD_PROJECT      DECIMAL(12) not null,
    FK_DCD_REPORT       DECIMAL(12) not null,
    FK_DCD_INFO         DECIMAL(15) not null,
    FK_DCD_REPORT_INTSN INTEGER     not null,
    BATCH_ID            CHAR(5)     not null,
    VALUE               VARCHAR(255),
    constraint IXU_DCD_048
        primary key (FK_DCD_PROJECT, FK_DCD_REPORT, FK_DCD_INFO, FK_DCD_REPORT_INTSN, BATCH_ID)
);

