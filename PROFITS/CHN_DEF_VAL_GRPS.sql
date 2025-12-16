create table CHN_DEF_VAL_GRPS
(
    ID_TRANSACT  INTEGER not null,
    ID_CHANNEL   INTEGER not null,
    ID           INTEGER not null
        constraint PK_CHN_DEF_VAL_GRPS
            primary key,
    ENTRY_STATUS CHAR(1) not null
);

