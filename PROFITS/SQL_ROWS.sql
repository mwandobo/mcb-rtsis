create table SQL_ROWS
(
    TIMESTMP      TIMESTAMP(6) not null,
    INT_SN        DECIMAL(10)  not null,
    FK_DCD_POJECT DECIMAL(12)  not null,
    FK_DCD_REPORT DECIMAL(12)  not null,
    FK_DCD_INFO   DECIMAL(15)  not null,
    DATA0         VARCHAR(2048),
    constraint PK_ROWS
        primary key (FK_DCD_POJECT, FK_DCD_REPORT, FK_DCD_INFO, INT_SN, TIMESTMP)
);

create unique index SQL_ROW_TMP
    on SQL_ROWS (TIMESTMP);

