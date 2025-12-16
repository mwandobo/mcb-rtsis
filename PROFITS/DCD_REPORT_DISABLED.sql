create table DCD_REPORT_DISABLED
(
    PROJECT_ID       DECIMAL(12) not null,
    REPORT_ID        DECIMAL(12) not null,
    RPROFITS_PROJECT DECIMAL(10),
    RPROFITS_REPORT  DECIMAL(10),
    STATUS           CHAR(1),
    DISABLE_TMSTAMP  TIMESTAMP(6),
    constraint PK_REP_DISABLE
        primary key (REPORT_ID, PROJECT_ID)
);

