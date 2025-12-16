create table EXTERNAL_IMPLEMENT_LOGIC
(
    ID                DECIMAL(10) not null
        constraint I0000317
            primary key,
    MEMBER_NAME       CHAR(8)     not null,
    FK_ACTION_BLOCKID DECIMAL(10)
);

create unique index I0000520
    on EXTERNAL_IMPLEMENT_LOGIC (FK_ACTION_BLOCKID);

