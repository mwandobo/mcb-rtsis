create table DFM_COMPOSER
(
    DFM_ID              INTEGER not null
        constraint PK_DFM_COMPOSER
            primary key,
    LATIN_IND           SMALLINT,
    MAIN_PTJ            SMALLINT,
    FK_DFM_MECHANISSNUM SMALLINT,
    FK_DFM_MECHANISID   INTEGER
);

