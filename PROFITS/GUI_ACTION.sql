create table GUI_ACTION
(
    CODE         CHAR(8),
    TMSTAMP      TIMESTAMP(6),
    ENTRY_STATUS CHAR(1),
    DESCRIPTION  CHAR(40)
);

create unique index IXU_GUI_000
    on GUI_ACTION (CODE);

