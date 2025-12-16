create table PROFITS_EXIT_STATE
(
    PRFT_SYSTEM        SMALLINT    not null,
    ID                 DECIMAL(12) not null,
    LANGUAGE0          INTEGER     not null,
    TERMINATION_ACTION CHAR(2),
    MESSAGE_TYPE       CHAR(2),
    ACTUAL_MESSAGE     VARCHAR(150),
    EXIT_STATE_DESC    CHAR(30)    not null,
    constraint PKPRFEXS
        primary key (LANGUAGE0, ID, PRFT_SYSTEM)
);

