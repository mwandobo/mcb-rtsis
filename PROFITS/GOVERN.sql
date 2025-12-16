create table GOVERN
(
    ID_PRODUCT      INTEGER      not null,
    ID_TRANSACT     INTEGER      not null,
    ID_JUSTIFIC     INTEGER      not null,
    TMSTAMP         TIMESTAMP(6) not null,
    ENTRY_STATUS    CHAR(1),
    FK_SEC_RULECODE INTEGER,
    constraint PKGOVERN
        primary key (ID_PRODUCT, ID_TRANSACT, ID_JUSTIFIC)
);

