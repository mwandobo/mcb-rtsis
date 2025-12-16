create table SQL_EXECUTE
(
    MULTI_ROW      SMALLINT     not null,
    TIMESTMP       TIMESTAMP(6) not null,
    INTERNAL_SN    DECIMAL(15)  not null,
    SHEET          INTEGER      not null,
    DATA_SQL       VARCHAR(4000),
    RESULT0        VARCHAR(4000),
    FK_DCD_PROJECT DECIMAL(12)  not null,
    FK_DCD_REPORT  DECIMAL(12)  not null,
    FK_DCD_INFO    DECIMAL(15)  not null,
    SQL_CRITERIA   VARCHAR(4000),
    constraint PK_RESULT
        primary key (FK_DCD_PROJECT, FK_DCD_REPORT, FK_DCD_INFO, TIMESTMP)
);

create unique index SQL_EXE_TMP
    on SQL_EXECUTE (TIMESTMP);

