create table XPAT_ACTION_TRANSACTION_MAP
(
    ID                  INTEGER not null
        constraint PATM0XPK
            primary key,
    SEQ_NUMBER          INTEGER not null,
    FK_XPAT_ACTIONTA_ID INTEGER,
    FK_XPAT_TRANSACID   INTEGER
);

comment on column XPAT_ACTION_TRANSACTION_MAP.SEQ_NUMBER is 'Number that defines the sequence of the Actions within the scenario';

create unique index PATM0XI1
    on XPAT_ACTION_TRANSACTION_MAP (FK_XPAT_ACTIONTA_ID);

create unique index PATM0XI2
    on XPAT_ACTION_TRANSACTION_MAP (FK_XPAT_TRANSACID);

