create table GLG_FINAL_DATE
(
    FK_GLG_FINALSUB_GH CHAR(5) not null,
    FK_GLG_FINALSUB_GD INTEGER not null,
    FINAL_DATE         DATE    not null,
    DATE_INTER_TRANS_F CHAR(1),
    constraint IXU_GL_031
        primary key (FK_GLG_FINALSUB_GH, FK_GLG_FINALSUB_GD, FINAL_DATE)
);

