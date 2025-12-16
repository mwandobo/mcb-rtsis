create table PAR_RELATION
(
    CODE             CHAR(8),
    TMSTAMP          TIMESTAMP(6),
    CONTROL_FLAG     CHAR(1),
    PARAMETER_TYPE_1 CHAR(5),
    PARAMETER_TYPE_2 CHAR(5),
    DESCRIPTION      VARCHAR(40),
    RSHIP_COMMENTS   VARCHAR(254)
);

create unique index IXU_PAR_004
    on PAR_RELATION (CODE);

