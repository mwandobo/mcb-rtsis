create table BDG_CNTR_UNIT
(
    FK_UNITCODE       INTEGER not null
        constraint IXU_GL_019
            primary key,
    STATUS            SMALLINT,
    LAST_UPD_YEAR     SMALLINT,
    TMSTMP            DATE,
    LAST_UPD_PERIOD   CHAR(2),
    FK_BDG_DISTRICTID CHAR(3),
    FILLER4           CHAR(180),
    FILLER3           CHAR(250),
    FILLER1           CHAR(250),
    FILLER2           CHAR(250)
);

create unique index I0000507
    on BDG_CNTR_UNIT (FK_BDG_DISTRICTID);

