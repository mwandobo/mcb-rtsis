create table SEC_RULE
(
    CODE         INTEGER      not null
        constraint PIXRULE
            primary key,
    TMSTAMP      TIMESTAMP(6) not null,
    ENTRY_STATUS CHAR(1),
    DESCRIPTION  CHAR(40)
);

