create table ALG_WEIGHTS
(
    ID          SMALLINT,
    MODULE0     SMALLINT,
    WEIGHTS     CHAR(20),
    DESCRIPTION CHAR(40)
);

create unique index IXU_ALG_000
    on ALG_WEIGHTS (ID);

