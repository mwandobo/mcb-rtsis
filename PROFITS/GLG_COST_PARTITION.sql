create table GLG_COST_PARTITION
(
    FK_GLG_COST_ID    CHAR(10) not null,
    FK_GLG_ACCOUNT_ID CHAR(21) not null,
    PERCENT           SMALLINT,
    DIFFERENCE        CHAR(1),
    constraint IXU_GLG_044
        primary key (FK_GLG_COST_ID, FK_GLG_ACCOUNT_ID)
);

