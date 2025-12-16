create table TEMP_EXCEL
(
    RECOR_SN      DECIMAL(10) not null,
    EXCEL_FILE_SN DECIMAL(10) not null,
    HEADER        CHAR(1),
    LAST_LINE     CHAR(1),
    PIG_LINE      VARCHAR(2048),
    constraint IXU_REP_216
        primary key (RECOR_SN, EXCEL_FILE_SN)
);

