create table REPORT_CRITERIA_DTL
(
    TYPE            CHAR(30)    not null,
    INTERNAL_SN     DECIMAL(10) not null,
    R_COLUMN        CHAR(5),
    R_ROW           DECIMAL(10),
    R_C_NUM_FROM    DECIMAL(15, 2),
    R_C_NUM_TO      DECIMAL(15, 2),
    R_C_TEXT_FROM   CHAR(10),
    R_C_TEXT_TO     CHAR(10),
    R_C_DATE_FROM   DATE,
    R_C_DATE_TO     DATE,
    R_C_DESCRIPTION VARCHAR(60)
);

