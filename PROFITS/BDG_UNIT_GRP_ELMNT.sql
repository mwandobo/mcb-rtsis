create table BDG_UNIT_GRP_ELMNT
(
    FK_BDG_GRP_ELMNBDG CHAR(4) not null,
    FK_BDG_CNTR_UNIFK  INTEGER not null,
    SIZE_PER           SMALLINT,
    RESULT_PER         SMALLINT,
    RATE_PER           DECIMAL(4, 2),
    TMSTMP             DATE,
    constraint IXU_GL_029
        primary key (FK_BDG_GRP_ELMNBDG, FK_BDG_CNTR_UNIFK)
);

create unique index I0000404
    on BDG_UNIT_GRP_ELMNT (FK_BDG_GRP_ELMNBDG);

create unique index I0000406
    on BDG_UNIT_GRP_ELMNT (FK_BDG_CNTR_UNIFK);

