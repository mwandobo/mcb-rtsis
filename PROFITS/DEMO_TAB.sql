create table DEMO_TAB
(
    COL1 INTEGER not null
        constraint PK_DEMO
            primary key,
    COL2 VARCHAR(100),
    COL3 DATE
);

create unique index IDX_DEMO_1
    on DEMO_TAB (COL1, COL2);

