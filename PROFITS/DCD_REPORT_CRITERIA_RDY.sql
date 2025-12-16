create table DCD_REPORT_CRITERIA_RDY
(
    FK_DCD_PROJECT DECIMAL(12) not null,
    FK_DCD_REPORT  DECIMAL(12) not null,
    FK_DCD_INFO    DECIMAL(15) not null,
    INTSN          DECIMAL(10) not null,
    DESCR          VARCHAR(100),
    SQL_CRITERIA   VARCHAR(1000),
    SQL_VISUAL     VARCHAR(1200),
    constraint IXU_DCD_012
        primary key (FK_DCD_PROJECT, FK_DCD_REPORT, FK_DCD_INFO, INTSN)
);

