create table WS_HEADER
(
    WS_CODE      VARCHAR(20) not null,
    DESCRIPTION  VARCHAR(80),
    ENTRY_STATUS CHAR(1),
    ONLINE_POST  CHAR(1) default '0'
);

create unique index PK_WS_HEADER
    on WS_HEADER (WS_CODE);

