create table BDG_D_ACT_MAPPING
(
    FK_BDG_H_ACT_UNIT  INTEGER  not null,
    FK_BDG_H_ACT_ELMNT CHAR(30) not null,
    FK_GLG_ACCOUNTACCO CHAR(21) not null,
    FK_GD_HAS_CATUN    INTEGER,
    ACTION             CHAR(1),
    UNIT_CAT           CHAR(1),
    AMOUNT_TYPE        CHAR(1),
    FK_GH_HAS_CATUN    CHAR(5),
    constraint IXU_GL_042
        primary key (FK_BDG_H_ACT_UNIT, FK_BDG_H_ACT_ELMNT, FK_GLG_ACCOUNTACCO)
);

