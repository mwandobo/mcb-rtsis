create table BDG_FCT_CONST
(
    ID          CHAR(3) not null
        constraint IXU_GL_053
            primary key,
    STATUS      SMALLINT,
    VALUE0      DECIMAL(15, 11),
    TMPSTMP     DATE,
    UPDATE_FLG  CHAR(1),
    DESCRIPTION VARCHAR(20)
);

