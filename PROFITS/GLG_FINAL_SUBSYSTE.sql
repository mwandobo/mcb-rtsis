create table GLG_FINAL_SUBSYSTE
(
    FK_GH_SUBSYSTEM    CHAR(5) not null,
    FK_GD_SUBSYSTEM    INTEGER not null,
    SYS_INTER_TRANS_FL CHAR(1),
    constraint IXU_GL_003
        primary key (FK_GH_SUBSYSTEM, FK_GD_SUBSYSTEM)
);

