create table GLG_FINAL_UNIT
(
    FK_GH_SUBSYSTEM    CHAR(5) not null,
    FK_GD_SUBSYSTEM    INTEGER not null,
    FK_GLG_FINAL_DATE  DATE    not null,
    FINAL_UNIT         INTEGER not null,
    UNIT_INTER_TRANS_F CHAR(1),
    constraint IXU_GL_004
        primary key (FK_GH_SUBSYSTEM, FK_GD_SUBSYSTEM, FK_GLG_FINAL_DATE, FINAL_UNIT)
);

