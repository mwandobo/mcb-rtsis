create table ATM_TERMINAL
(
    ATM_TERMINAL_ID CHAR(16) not null
        constraint IXU_ATM_036
            primary key,
    ATM_UNIT        INTEGER,
    DR_JUSTIFIC     INTEGER,
    ATM_USER        CHAR(8),
    ATM_LOCATION    CHAR(20)
);

