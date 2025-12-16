create table SEC_WINDOW
(
    CODE         CHAR(8),
    TMSTAMP      TIMESTAMP(6),
    ENTRY_STATUS CHAR(1),
    DESCRIPTION  CHAR(40)
);

create unique index IXU_SEC_007
    on SEC_WINDOW (CODE);

