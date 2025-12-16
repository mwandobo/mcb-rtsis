create table DCD_BATCH_REPORT_DETAILS
(
    IN_SN               INTEGER     not null,
    DATA                VARCHAR(2048),
    FK_DCD_BATCH_RESN   SMALLINT    not null,
    FK_BAT_EXEC_DATE    DATE        not null,
    FK_DCD_BAT_BATCH_ID CHAR(5)     not null,
    SHEETROWCOL         VARCHAR(15),
    REP_INFO_INT_SN     DECIMAL(15) not null,
    constraint IXU_DCD_017
        primary key (FK_DCD_BATCH_RESN, FK_BAT_EXEC_DATE, FK_DCD_BAT_BATCH_ID, IN_SN)
);

