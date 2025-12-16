create table BOP_ITEM
(
    DESCRIPTION        VARCHAR(40),
    TMSTAMP            TIMESTAMP(6),
    ENTRY_STATUS       CHAR(1),
    GROUP_AMOUNT       DECIMAL(15, 2),
    GROUP_TYPE         CHAR(1),
    BOP_ITEM_CODE      INTEGER not null
        constraint PKBOPITE
            primary key,
    FK_CURR_DEFINED_BY INTEGER
);

