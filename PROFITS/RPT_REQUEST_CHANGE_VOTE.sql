create table RPT_REQUEST_CHANGE_VOTE
(
    REQUEST_ID BIGINT       not null,
    USER_CODE  VARCHAR(10)  not null,
    RATE       SMALLINT,
    CREATED    TIMESTAMP(6) not null,
    UPDATED    TIMESTAMP(6) not null,
    primary key (REQUEST_ID, USER_CODE)
);

