create table VER_ISSUES
(
    VERSION_DATE         VARCHAR(6)  not null,
    VERSION_NUMBER       VARCHAR(4)  not null,
    ISSUE_NUMBER         INTEGER     not null,
    ISSUE_PREFIX         VARCHAR(30) not null,
    ISSUE_NUMBER_CHECKED CHAR(1),
    ISSUE_STATUS         SMALLINT,
    PROJECT_ID           DECIMAL(11) not null,
    VERSION_CODE         VARCHAR(12) not null,
    constraint SYS_C0019661
        primary key (VERSION_DATE, VERSION_NUMBER, ISSUE_NUMBER, ISSUE_PREFIX)
);

