create table DCD_REPORT_PROJECT
(
    PROJECT_ID  DECIMAL(12) not null
        constraint IXU_DCD_040
            primary key,
    HEADER_DESC CHAR(40),
    HEADER_PATH CHAR(200)
);

