create table PIG_TO_EXCEL_MAP
(
    FK_PIG_ASCFILE_FORMAT_ID CHAR(5) not null,
    FIELD_SN                 INTEGER not null,
    COL                      INTEGER,
    H_ROW                    INTEGER,
    FORMAT                   VARCHAR(100),
    constraint IXU_PRD_020
        primary key (FK_PIG_ASCFILE_FORMAT_ID, FIELD_SN)
);

