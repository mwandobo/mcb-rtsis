create table TR_AGENDA_HD
(
    AGENDA_CODE INTEGER not null
        constraint IXU_DEP_160
            primary key,
    AGENDA_DESC VARCHAR(50)
);

