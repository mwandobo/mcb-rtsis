create table BDG_GRP_ELMNT_D
(
    FK_BDG_ELEMENTID   CHAR(30) not null,
    FK_BDG_GRP_ELMNBDG CHAR(4)  not null,
    TMSTMP             DATE,
    constraint IXU_GL_046
        primary key (FK_BDG_ELEMENTID, FK_BDG_GRP_ELMNBDG)
);

create unique index I0000398
    on BDG_GRP_ELMNT_D (FK_BDG_ELEMENTID);

create unique index I0000400
    on BDG_GRP_ELMNT_D (FK_BDG_GRP_ELMNBDG);

