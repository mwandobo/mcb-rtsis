create table RPT_REPORT_RESULT_FILE
(
    FK_REPORT_RESULT_ID BIGINT  not null,
    SN                  INTEGER not null,
    FK_FILE_ID          INTEGER default 0,
    BREAK_LABEL         VARCHAR(50),
    BREAK_VALUE         VARCHAR(4000),
    constraint RPT_REPORT_RESULT_FILE_PK
        primary key (FK_REPORT_RESULT_ID, SN)
);

