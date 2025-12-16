create table CGN_TABLES
(
    TABLE_SN           DECIMAL(10)  not null
        constraint PK_CGN_TABLES
            primary key,
    FLD_TABLE_TYPE     CHAR(1)      not null,
    FLD_TABLE_ALIAS    VARCHAR(50),
    FLD_SELECT_KEY     VARCHAR(100) not null,
    FLD_TABLE          VARCHAR(200) not null,
    FLD_JOIN_CONDITION VARCHAR(400)
);

