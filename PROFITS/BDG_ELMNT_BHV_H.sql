create table BDG_ELMNT_BHV_H
(
    ID          SMALLINT not null
        constraint IXU_GL_022
            primary key,
    STATUS      SMALLINT,
    TMSTMP      DATE,
    DESCRIPTION VARCHAR(40)
);

