create table BDG_GRP_ELMNT_H
(
    BDG_GRP_ID CHAR(4) not null
        constraint IXU_GL_026
            primary key,
    TMPSTMP    DATE,
    STATUS     CHAR(1),
    BASE       CHAR(1),
    DESCR      VARCHAR(30)
);

