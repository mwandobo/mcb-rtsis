create table SCH_ITEM_FLOW
(
    FK_SCRIPT VARCHAR(40) not null,
    FROM_ITEM VARCHAR(40) not null,
    TO_ITEM   VARCHAR(40) not null,
    constraint SCH_ITEM_FLOW_PK
        primary key (FK_SCRIPT, FROM_ITEM, TO_ITEM)
);

