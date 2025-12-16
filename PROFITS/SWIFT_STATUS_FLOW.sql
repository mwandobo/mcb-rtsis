create table SWIFT_STATUS_FLOW
(
    MSG_CATEGORY      CHAR(1) not null
        constraint SW_STFL_PK
            primary key,
    CURRENT_STATUS    CHAR(1),
    CURR_STATUS_DESCR VARCHAR(20),
    NEXT_STATUS       CHAR(1),
    NEXT_STATUS_DESCR VARCHAR(20),
    ACTION            VARCHAR(20)
);

