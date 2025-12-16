create table BDG_FORMULA
(
    FK_BDG_ELEMENTID CHAR(30) not null,
    TYPE             CHAR(1)  not null,
    STATUS           SMALLINT,
    COMP_ORDER       SMALLINT,
    TMSTMP           DATE,
    INFIX_FORMULA    VARCHAR(300),
    POSTFIX_FORMULA  VARCHAR(600),
    constraint IXU_GL_045
        primary key (FK_BDG_ELEMENTID, TYPE)
);

