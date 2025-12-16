create table PIG_MAP_REG
(
    REG           VARCHAR(100) not null,
    PROJECT_TYPE  SMALLINT     not null,
    KIND          SMALLINT,
    PRFT_CODE     INTEGER,
    STATUS        CHAR(1),
    TRANZ_ACCOUNT VARCHAR(25),
    VILLAGE       VARCHAR(25),
    constraint IXU_PRD_017
        primary key (REG, PROJECT_TYPE)
);

