create table TEMP_COST_OPTION
(
    DATE_FROM      DATE     not null,
    DATE_TO        DATE     not null,
    PURPOSE_CODE   INTEGER  not null,
    DURATION_FROM  SMALLINT not null,
    DURATION_TO    SMALLINT not null,
    COST_OF_OPTION DECIMAL(9, 6),
    constraint TEMP_COST_OPTION_PK
        primary key (DATE_FROM, DATE_TO, DURATION_TO, DURATION_FROM, PURPOSE_CODE)
);

