create table PIG_ROW_INDICATOR
(
    INDICATOR     CHAR(10) not null,
    ORGANIZATION  CHAR(10) not null,
    FIELD_SECTION SMALLINT,
    constraint IXU_PRD_019
        primary key (INDICATOR, ORGANIZATION)
);

