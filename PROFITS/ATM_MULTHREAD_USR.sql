create table ATM_MULTHREAD_USR
(
    PORT      VARCHAR(10) not null,
    CHANNEL   CHAR(1)     not null,
    USED      CHAR(1),
    USR       CHAR(8),
    ENTRY_STS CHAR(1),
    constraint PK_ATM_MULTITHREAD
        primary key (CHANNEL, PORT)
);

