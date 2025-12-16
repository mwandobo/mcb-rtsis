create table TR_AGENDA_DT
(
    FK_TR_AGENDA_CODE INTEGER  not null,
    AGENDA_LINE       SMALLINT not null,
    AGENDA_COMMENTS   VARCHAR(50),
    constraint IXU_DEP_144
        primary key (FK_TR_AGENDA_CODE, AGENDA_LINE)
);

