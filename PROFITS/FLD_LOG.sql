create table FLD_LOG
(
    USER_CODE      VARCHAR(10) default '0' not null,
    PROCESS_DATE   DATE                    not null,
    FILE_PATH      VARCHAR(400)            not null,
    DESTINATION_ID INTEGER                 not null,
    DURATION_TIME  TIME,
    LINES_INSERTED INTEGER     default 0   not null,
    ERROR          CLOB(1048576),
    PROCESS_STATUS INTEGER     default 0   not null,
    constraint FK_LOADING
        primary key (USER_CODE, PROCESS_DATE, DESTINATION_ID)
);

