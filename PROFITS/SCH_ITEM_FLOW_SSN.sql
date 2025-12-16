create table SCH_ITEM_FLOW_SSN
(
    SESSION_ID TIMESTAMP(6) not null,
    FK_SCRIPT  VARCHAR(40)  not null,
    FROM_ITEM  VARCHAR(40)  not null,
    TO_ITEM    VARCHAR(40)  not null
);

