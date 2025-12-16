create table FIN_INSTR_ROUND
(
    INSTRUMENT_TYPE CHAR(2)     not null,
    ID_CURRENCY     INTEGER     not null,
    DESCRIPTION     VARCHAR(40) not null,
    MIN_NEGOT_UNIT  SMALLINT    not null,
    COEFFICIENT     SMALLINT    not null,
    constraint PK_FIN_INSTR_TYPE
        primary key (INSTRUMENT_TYPE, ID_CURRENCY)
);

