create table DCD_TRNS_LANGUAGE
(
    CODE             CHAR(8)      not null,
    ID               INTEGER      not null,
    TMPSTAMP         TIMESTAMP(6) not null,
    STATUS0          CHAR(1),
    PASSWORD         CHAR(26),
    LANG_DESCRIPTION CHAR(40),
    constraint IXU_DEF_123
        primary key (CODE, ID, TMPSTAMP)
);

