create table PERM_STS_CHANNEL
(
    ID_CHANNEL         INTEGER not null,
    ID_TRANSACT        INTEGER not null,
    ID_JUSTIFIC        INTEGER not null,
    ACC_STATUS         CHAR(1) not null,
    ALL_TRANSACTIONS   CHAR(1),
    ALL_JUSTIFICATIONS CHAR(1),
    constraint PK_PERM_STS_CHANNEL
        primary key (ACC_STATUS, ID_JUSTIFIC, ID_TRANSACT, ID_CHANNEL)
);

