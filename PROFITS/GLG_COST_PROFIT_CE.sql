create table GLG_COST_PROFIT_CE
(
    COST_ID        CHAR(10) not null
        constraint IXU_GLG_008
            primary key,
    TMSTAMP        TIMESTAMP(6),
    ENTRY_STATUS   CHAR(1),
    COST_TYPE_FLAG CHAR(1),
    DESCRIPTION    CHAR(30)
);

