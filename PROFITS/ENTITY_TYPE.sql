create table ENTITY_TYPE
(
    ID              DECIMAL(10) not null
        constraint ID2
            primary key,
    NAME            CHAR(32)    not null,
    DSD_NAME        VARCHAR(32) not null,
    MIN_OCCUR       DECIMAL(10) not null,
    MAX_OCCUR       DECIMAL(10) not null,
    AVG_OCCUR       DECIMAL(10) not null,
    GROWTH_RATE     INTEGER     not null,
    GROWTH_RATE_PER CHAR(1)     not null,
    FK_MODELID      DECIMAL(10)
);

create unique index I0000544
    on ENTITY_TYPE (FK_MODELID);

