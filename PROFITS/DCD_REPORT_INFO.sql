create table DCD_REPORT_INFO
(
    FK_DCD_REPORTFK_DCD_REPORT_PPR DECIMAL(12) not null,
    FK_DCD_REPORTREPORT_ID         DECIMAL(12) not null,
    INTERNAL_SN                    DECIMAL(15) not null,
    MULTI_ROW                      SMALLINT,
    BREAKSHEETPERCOL               SMALLINT,
    ROW_NUMBER                     INTEGER,
    SHEET                          INTEGER,
    COLUMN_NUMBER                  INTEGER,
    DATA_DESCRIPTION               VARCHAR(4000),
    DATA_SQL                       VARCHAR(4000),
    constraint IXU_DCD_013
        primary key (FK_DCD_REPORTFK_DCD_REPORT_PPR, FK_DCD_REPORTREPORT_ID, INTERNAL_SN)
);

