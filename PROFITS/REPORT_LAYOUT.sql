create table REPORT_LAYOUT
(
    PAPER_WIDTH        INTEGER  not null,
    PAPER_H            INTEGER  not null,
    MARGIN_BOTTOM      INTEGER  not null,
    MARGIN_TOP         INTEGER  not null,
    MARGIN_RIGHT       INTEGER  not null,
    MARGIN_LEFT        INTEGER  not null,
    FK_RPT_REPORTREPOR CHAR(15) not null
        constraint I0000706
            primary key
);

