create table SCH_ITEM_FLOW_BCK
(
    TIMESTAMP_BCK TIMESTAMP(6) not null,
    USER_BCK      VARCHAR(20)  not null,
    FK_SCRIPT     VARCHAR(40)  not null,
    FROM_ITEM     VARCHAR(40)  not null,
    TO_ITEM       VARCHAR(40)  not null
);

create unique index SCH_ITEM_FLOW_BCK_PK
    on SCH_ITEM_FLOW_BCK (FK_SCRIPT, TIMESTAMP_BCK);

