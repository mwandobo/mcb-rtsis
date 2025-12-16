create table CELL_DIMENSION
(
    WIDTH              INTEGER      not null,
    POSITION_IN_LINE   INTEGER      not null,
    FK_CELLFK_LINETIME TIMESTAMP(6) not null,
    FK_CELLCELL_NUMBER SMALLINT     not null,
    constraint I0000687
        primary key (FK_CELLCELL_NUMBER, FK_CELLFK_LINETIME)
);

