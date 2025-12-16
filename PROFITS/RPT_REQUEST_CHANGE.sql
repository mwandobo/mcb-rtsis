create table RPT_REQUEST_CHANGE
(
    ID               BIGINT        not null
        primary key,
    REPORT_ID        INTEGER       not null,
    USER_CODE        VARCHAR(10)   not null,
    REQUEST_DESCR    VARCHAR(4000) not null,
    VOTES            SMALLINT,
    RATE             DECIMAL(2, 1),
    STATUS           SMALLINT      not null,
    PROGRESS_USER    VARCHAR(10),
    ATTACH_DOC       BLOB(1048576),
    ATTACH_FILENAME  VARCHAR(100),
    ATTACH_EXTENSION VARCHAR(20),
    CREATED          TIMESTAMP(6)  not null,
    UPDATED          TIMESTAMP(6)  not null
);

create unique index RPT_REQ_CHANGE_IDX
    on RPT_REQUEST_CHANGE (USER_CODE);

create unique index RPT_REQ_CHANGE_RPTID_IDX
    on RPT_REQUEST_CHANGE (REPORT_ID);

