create table DCD_TRNS_VOUCH_FRM
(
    CODE        CHAR(8)      not null,
    FORMAT_NAME CHAR(30)     not null,
    TMPSTAMP    TIMESTAMP(6) not null,
    VAR_TYPE    SMALLINT,
    STATUS      CHAR(1),
    PASSWORD    CHAR(26),
    DESCRIPTION CHAR(80),
    constraint IXU_DEF_080
        primary key (CODE, FORMAT_NAME, TMPSTAMP)
);

