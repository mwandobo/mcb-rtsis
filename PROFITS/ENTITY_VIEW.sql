create table ENTITY_VIEW
(
    ID          DECIMAL(10) not null
        constraint ID3
            primary key,
    NAME        CHAR(32)    not null,
    SEQ         DECIMAL(10) not null,
    USED_AS_I_O CHAR(1)     not null,
    ENTITY_ID   DECIMAL(10) not null,
    PARENT_ID   DECIMAL(10) not null
);

