create table PIG_MAP_RAIONS
(
    VILLAGE       VARCHAR(100) not null,
    DISTRICT      VARCHAR(100) not null,
    RSSTATUS      SMALLINT,
    KIND          SMALLINT,
    PRFT_CODE     INTEGER,
    TRANZ_ACCOUNT VARCHAR(50),
    constraint IXU_PRD_016
        primary key (VILLAGE, DISTRICT)
);

