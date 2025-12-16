create table RPT_LINE
(
    TIMESTAMP_ID       TIMESTAMP(6) not null
        constraint I0000604
            primary key,
    LINE_NUMBER        SMALLINT     not null,
    LINE_TYPE          CHAR(1)      not null,
    REPEATABLE_AFTER_P CHAR(1),
    SPECIAL_TOTAL_ZONE CHAR(1)      not null,
    WIDTH              INTEGER      not null,
    NUMBER_OF_CARACTER SMALLINT,
    WITH_BORDER        CHAR(1)      not null,
    CELL_NUMBER        SMALLINT     not null,
    REPETITIVE         CHAR(1)      not null,
    INCLUDED_IN_REPETI SMALLINT,
    NEW_PAGE_REQUIRED  CHAR(1),
    TO_BE_REPEATED_FOR CHAR(1),
    OPTIONALITY        CHAR(1)
);

