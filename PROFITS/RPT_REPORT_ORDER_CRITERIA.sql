create table RPT_REPORT_ORDER_CRITERIA
(
    FK_REPORT_ORDER_ID INTEGER            not null,
    FK_REPORT_ID       INTEGER            not null,
    ID                 INTEGER            not null,
    EXECUTION_VALUE    VARCHAR(500)       not null,
    CRITERIA_TYPE      SMALLINT default 0 not null,
    constraint RPT_REPORT_ORDER_CRITERIA_PK
        primary key (FK_REPORT_ORDER_ID, FK_REPORT_ID, ID, CRITERIA_TYPE)
);

