create table BDG_FACTORS_H
(
    ID                 CHAR(3) not null
        constraint IXU_GL_044
            primary key,
    STATUS             SMALLINT,
    LAST_UPDATE_YEAR   SMALLINT,
    TMSTMP             DATE,
    LAST_UPDATE_PERIOD CHAR(2),
    DESCRIPTION        VARCHAR(20)
);

