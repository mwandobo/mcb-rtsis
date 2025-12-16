create table BDG_H_ACT_MAPPING
(
    FK_BDG_CNTR_UNIFK INTEGER  not null
        constraint FK_BDG_H00A
            references BDG_CNTR_UNIT,
    FK_BDG_ELEMENTID  CHAR(30) not null
        constraint FK_BDG_H00B
            references BDG_ELEMENT,
    TMSTMP            TIMESTAMP(6),
    STATUS            CHAR(1),
    constraint IXU_GL_027
        primary key (FK_BDG_CNTR_UNIFK, FK_BDG_ELEMENTID)
);

