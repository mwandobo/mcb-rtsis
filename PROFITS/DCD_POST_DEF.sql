create table DCD_POST_DEF
(
    COUNTER           INTEGER not null,
    FK_DCD_DYN_GUI_ID INTEGER not null,
    FK_DCD_GEN_DEF_ID INTEGER not null,
    ORDERING          INTEGER,
    constraint IXU_DEF_053
        primary key (COUNTER, FK_DCD_DYN_GUI_ID, FK_DCD_GEN_DEF_ID)
);

