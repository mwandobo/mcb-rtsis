create table DCD_REPORT_CRITERIA
(
    FK_DCD_PROJECT DECIMAL(12) not null,
    FK_DCD_REPORT  DECIMAL(12) not null,
    FK_DCD_INFO    DECIMAL(15) not null,
    INTSN          INTEGER     not null,
    PER_SHEET      SMALLINT,
    FIELD_TYPE     SMALLINT,
    FIELD          VARCHAR(40),
    LABEL          VARCHAR(40),
    DYNAMIC_SQL    VARCHAR(1000),
    DEFAULT0       VARCHAR(1000),
    constraint IXU_DCD_011
        primary key (FK_DCD_PROJECT, FK_DCD_REPORT, FK_DCD_INFO, INTSN)
);

create unique index IXU_DCD_REPORT_CRITERIA
    on DCD_REPORT_CRITERIA (FK_DCD_REPORT, FK_DCD_PROJECT, FIELD);

