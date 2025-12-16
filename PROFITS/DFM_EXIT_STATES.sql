create table DFM_EXIT_STATES
(
    ID                 INTEGER     not null
        constraint PK_DFM_EXIT_STATE
            primary key,
    EXIT_STATE_TEXT    VARCHAR(40) not null,
    TERMINATION_ACTION CHAR(1),
    MESSAGE_TYPE       CHAR(1)
);

