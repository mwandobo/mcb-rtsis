create table PAT_ACTION_TRANSACTION_MAP
(
    ID                  INTEGER not null
        constraint PATM01PK
            primary key,
    SEQ_NUMBER          INTEGER not null,
    FK_PAT_ACTIONSTA_ID INTEGER,
    FK_PAT_TRANSACTID   INTEGER
);

create unique index PATM01I2
    on PAT_ACTION_TRANSACTION_MAP (FK_PAT_TRANSACTID);

create unique index PATM01I3
    on PAT_ACTION_TRANSACTION_MAP (FK_PAT_ACTIONSTA_ID);

