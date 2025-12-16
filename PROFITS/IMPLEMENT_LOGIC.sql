create table IMPLEMENT_LOGIC
(
    MODEL_ID          DECIMAL(10) not null,
    ID                DECIMAL(10) not null
        constraint ID7
            primary key,
    MEMBER_NAME       CHAR(8)     not null,
    FK_ACTION_BLOCKID DECIMAL(10)
);

create unique index I0000564
    on IMPLEMENT_LOGIC (FK_ACTION_BLOCKID);

