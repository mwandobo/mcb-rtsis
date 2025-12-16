create table DCD_REPORT
(
    FK_DCD_REPORT_PPROJECT_ID DECIMAL(12) not null,
    REPORT_ID                 DECIMAL(12) not null,
    TEMPLATE_FILE_SN          DECIMAL(15),
    TMSTAMP                   TIMESTAMP(6),
    DETAIL_DESC               CHAR(40),
    RESULT_IN_EXCEL           CHAR(200),
    RESULT_IN_TEXT            CHAR(200),
    TEMPLATE_IN_EXCEL         VARCHAR(200),
    FULL_DESC                 VARCHAR(4000),
    TEMPLATE_FILE             BLOB(1048576),
    constraint IXU_DCD_010
        primary key (FK_DCD_REPORT_PPROJECT_ID, REPORT_ID)
);

